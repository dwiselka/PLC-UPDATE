import paramiko
import tkinter as tk
from tkinter import filedialog, messagebox, ttk, scrolledtext
import threading
import os
import time
from datetime import datetime
import pytz
import sys
import openpyxl
from openpyxl.styles import PatternFill, Font
import queue
from contextlib import contextmanager

# cd "C:\Users\dawid.wiselka\OneDrive - NOMAD ELECTRIC Sp. z o.o\Dokumenty\Farmy\Updater\all"
# python FirmwareUpdater_listaExcel.py
# pyinstaller --onefile --noconsole --icon="plcv2.ico" --add-data "plcv2.ico;." --add-data "Default.scm.config;." FirmwareUpdater_listaExcel.py




# Konfiguracja
PLC_USER = "admin"
ROOT_PASS = "12345"
TIMEZONE = "Europe/Warsaw"
SYSTEM_SERVICES_FILE = "Default.scm.config"
RETRY_ATTEMPTS = 3
RETRY_DELAY = 10  # sekund

def resource_path(relative_path):
    """Zwraca absolutną ścieżkę do pliku, działa również w exe PyInstaller."""
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

class PLCDevice:
    """Klasa reprezentująca jeden sterownik PLC."""
    def __init__(self, name, ip, password):
        self.name = name
        self.ip = ip
        self.password = password
        self.firmware_version = ""
        self.timezone = ""
        self.system_services_ok = ""
        self.last_check = ""
        self.last_update = ""
        self.status = "Oczekuje"
        self.error_log = ""
        self.plc_model = ""
        self.plc_time = ""
        self.time_sync_error = False

class BatchProcessorApp(tk.Tk):
    """Główna aplikacja do przetwarzania wsadowego sterowników PLC."""

    def __init__(self):
        super().__init__()
        self.title("PLC Batch Updater - Phoenix Contact")
        self.geometry("1200x900")
        try:
            self.iconbitmap(resource_path("plcv2.ico"))
        except:
            pass

        # Zmienne stanu
        self.excel_path = tk.StringVar()
        self.firmware_path = tk.StringVar()
        self.devices = []
        self.processing = False
        self.log_queue = queue.Queue()
        
        # Tworzenie GUI
        self.create_widgets()
        
        # Timer do aktualizacji logów
        self.update_logs()


    @contextmanager
    def ssh_connection(self, device):
        """
        Context manager dla bezpiecznego zarządzania połączeniem SSH.
        Automatycznie zamyka połączenie nawet przy błędach.
        
        Użycie:
            with self.ssh_connection(device) as (ssh, sftp):
                # ... operacje ...
        """
        ssh = None
        sftp = None
        
        try:
            self.log(f"  🔗 Otwieranie połączenia SSH do {device.ip}...")
            
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                device.ip, 
                username=PLC_USER, 
                password=device.password, 
                timeout=30,
                banner_timeout=30,
                auth_timeout=30,
                allow_agent=False,
                look_for_keys=False
            )
            
            sftp = ssh.open_sftp()
            
            self.log(f"  ✓ Połączono z {device.ip}")
            
            yield ssh, sftp
            
        except Exception as e:
            self.log(f"  ❌ Błąd połączenia SSH: {str(e)}")
            raise
            
        finally:
            # Zamknij SFTP
            if sftp:
                try:
                    sftp.close()
                    self.log(f"  🔒 Zamknięto SFTP")
                    time.sleep(0.7)
                except Exception as e:
                    self.log(f"  ⚠️  Błąd zamykania SFTP: {str(e)}")
            
            # Zamknij SSH
            if ssh:
                try:
                    transport = ssh.get_transport()
                    if transport and transport.is_active():
                        transport.close()
                    ssh.close()
                    self.log(f"  🔒 Zamknięto SSH")
                    time.sleep(1) 
                except Exception as e:
                    self.log(f"  ⚠️  Błąd zamykania SSH: {str(e)}")
            

            time.sleep(2)


    def execute_firmware_update(self, device):
        """
        Wykonuje sudo update firmware (tworzy NOWE połączenie SSH).
        """
        ssh = None
        channel = None
        try:
            self.log(f"  🔗 Nowe połączenie SSH dla firmware update...")
            
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                device.ip, 
                username=PLC_USER, 
                password=device.password, 
                timeout=30
            )
            
            update_command = f"sudo update-axcf{device.plc_model}"
            self.log(f"  ⚠️ Uruchamiam: {update_command}")
            self.log(f"  ⏳ Czekam na zakończenie procesu update (może zająć kilka minut)...")
            
            channel = ssh.get_transport().open_session()
            channel.get_pty()
            channel.exec_command(update_command)
            
            channel.send(device.password + "\n")
            
            output = ""
            start_time = time.time()
            timeout = 300  # 5 minut
            
            while True:
                if time.time() - start_time > timeout:
                    self.log("  ⚠️ Timeout - przekroczono 5 minut oczekiwania")
                    break
                
                if channel.recv_ready():
                    chunk = channel.recv(1024).decode(errors="ignore")
                    output += chunk
                    for line in chunk.split('\n'):
                        if line.strip() and any(keyword in line.lower() for keyword in 
                            ['installing', 'updating', 'done', 'success', 'error', 'failed', 'reboot']):
                            self.log(f"    {line.strip()}")
                
                if channel.exit_status_ready():
                    exit_code = channel.recv_exit_status()
                    self.log(f"  ✓ Proces zakończony z kodem: {exit_code}")
                    break
                
                time.sleep(0.5)
            
            if channel.recv_stderr_ready():
                errors = channel.recv_stderr(4096).decode(errors="ignore")
                if errors.strip():
                    self.log(f"  ⚠️ Stderr: {errors[:200]}")
            
            channel.close()
            ssh.close()
            
            self.log("  ✓ Aktualizacja firmware zakończona. Sterownik restartuje się")
            self.log("  ⏳ Czekam 30s na restart sterownika...")
            time.sleep(30)
            
        except Exception as e:
            raise e
        finally:
            if channel:
                try:
                    channel.close()
                    self.log("  🔒 Zamknięto kanał SSH")
                except:
                    pass
            if ssh:
                try:
                    ssh.close()
                    self.log("  🔒 Zamknięto SSH")
                except:
                    pass
            time.sleep(10)

    def execute_reboot(self, device):
        ssh = None
        try:
            self.log(f"  🔗 Nowe połączenie SSH dla reboot...")
            
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                device.ip, 
                username=PLC_USER, 
                password=device.password, 
                timeout=30,
                banner_timeout=30,
                auth_timeout=30
            )
            
            self.log("  ⚠️ Uruchamiam 'sudo reboot'...")
            
            stdin, stdout, stderr = ssh.exec_command("sudo reboot", get_pty=True)
            stdin.write(device.password + "\n")
            stdin.flush()
            time.sleep(2)
            
        except Exception as e:
            # Ignoruj błędy zamknięcia - reboot ich powoduje
            if "Socket is closed" in str(e) or "Timeout" in str(e) or "EOF" in str(e):
                self.log("  ✓ Reboot zainicjowany (połączenie przerwane - oczekiwane)")
            else:
                raise e
        finally:
            if ssh:
                try:
                    ssh.close()
                    self.log("  🔒 Zamknięto SSH po reboot")
                except:
                    pass
            time.sleep(1)
            
        # Czekaj na restart
        self.log("  ⏳ Czekam 30s na restart sterownika...")
        time.sleep(60)




    def upload_callback(self, filename, transferred, total):
        """
        Callback wywoływany podczas uploadu pliku przez SFTP.
        Aktualizuje progress bar i status.
        """
        if total > 0:
            percent = (transferred / total) * 100
            
            # Aktualizuj progress bar
            self.upload_progress['value'] = percent
            
            # Oblicz rozmiary w MB
            transferred_mb = transferred / 1024 / 1024
            total_mb = total / 1024 / 1024
            
            # Formatuj status
            status_text = f"📤 {filename}: {transferred_mb:.1f} MB / {total_mb:.1f} MB ({percent:.1f}%)"
            
            # Aktualizuj GUI (thread-safe)
            self.after(0, lambda: self.upload_status_label.config(
                text=status_text, 
                fg="blue"
            ))
            
            # Log co 10%
            if int(percent) % 10 == 0 and int(percent) > 0:
                self.log(f"  📊 Upload: {percent:.0f}% ({transferred_mb:.1f}/{total_mb:.1f} MB)")

    def reset_upload_progress(self):
        """Resetuje progress bar po zakończeniu uploadu."""
        self.after(0, lambda: self.upload_progress.config(value=0))
        self.after(0, lambda: self.upload_status_label.config(
            text="Oczekiwanie na transfer...",
            fg="gray"
        ))



    def process_batch(self, operation):
        """
        Główna metoda przetwarzania wsadowego.
        operation: "read", "system_services", "timezone", "firmware", "all"
        """
        self.processing = True
        self.stop_btn.config(state="normal")
        
        total = len(self.devices)
        success_count = 0
        failed_count = 0
        
        self.log(f"{'='*60}")
        self.log(f"🚀 START OPERACJI WSADOWEJ: {operation.upper()}")
        self.log(f"📊 Liczba sterowników: {total}")
        self.log(f"{'='*60}")
        
        for idx, device in enumerate(self.devices, 1):
            if not self.processing:
                self.log("⏹️  Operacja zatrzymana przez użytkownika")
                break
            
            # OPÓŹNIENIE MIĘDZY STEROWNIKAMI (oprócz pierwszego)
            if idx > 1:
                self.log(f"\n⏳ Czekam 5 sekund przed kolejnym sterownikiem...")
                time.sleep(5)
            
            self.log(f"\n{'='*60}")
            self.log(f"[{idx}/{total}] 🔧 Przetwarzanie: {device.name} ({device.ip})")
            self.log(f"{'='*60}")
            
            device.status = "W trakcie..."
            device.error_log = ""
            self.after(0, lambda d=device: self.update_device_row(d))
            
            attempt = 0
            success = False
            
            while attempt < RETRY_ATTEMPTS and not success:
                attempt += 1
                
                if attempt > 1:
                    self.log(f"⚠️  Próba {attempt}/{RETRY_ATTEMPTS}")
                    time.sleep(RETRY_DELAY)
                
                try:
                    if operation == "read":
                        self.read_single_device(device)
                        success = True
                        
                    elif operation == "system_services":
                        self.update_system_services_only(device)
                        success = True
                        
                    elif operation == "timezone":
                        self.update_timezone_only(device)
                        success = True
                        
                    elif operation == "firmware":
                        self.update_firmware_only_operation(device)
                        success = True
                        
                    elif operation == "all":
                        self.update_all_operations(device)
                        success = True
                    
                    if success:
                        device.status = "✓ OK"
                        success_count += 1
                        self.log(f"✓ [{device.name}] Operacja zakończona sukcesem")
                        
                except Exception as e:
                    error_msg = str(e)
                    device.error_log = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {error_msg}"
                    
                    if attempt < RETRY_ATTEMPTS:
                        self.log(f"✗ Błąd (próba {attempt}/{RETRY_ATTEMPTS}): {error_msg}")
                    else:
                        device.status = "✗ Błąd"
                        failed_count += 1
                        self.log(f"✗ [{device.name}] Operacja nieudana po {RETRY_ATTEMPTS} próbach: {error_msg}")
                
                finally:
                    self.after(0, lambda d=device: self.update_device_row(d))
        
        # Podsumowanie
        self.log(f"\n{'='*60}")
        self.log(f"📊 PODSUMOWANIE OPERACJI: {operation.upper()}")
        self.log(f"{'='*60}")
        self.log(f"✓ Sukces: {success_count}/{total}")
        self.log(f"✗ Błędy: {failed_count}/{total}")
        self.log(f"{'='*60}\n")
        
        self.processing = False
        self.stop_btn.config(state="disabled")
        self.status_bar.config(text="Gotowy")
        
        # Pokaż podsumowanie
        self.after(0, lambda: messagebox.showinfo(
            "Operacja zakończona",
            f"Operacja: {operation.upper()}\n\n"
            f"✓ Sukces: {success_count}/{total}\n"
            f"✗ Błędy: {failed_count}/{total}\n\n"
            f"Sprawdź logi, aby uzyskać szczegóły."
        ))


    def read_single_device(self, device):
        """
        Odczytuje dane z pojedynczego sterownika.
        """
        try:
            with self.ssh_connection(device) as (ssh, sftp):
                
                # 1. Wykryj model PLC
                device.plc_model = self.detect_plc_model(ssh)
                
                # 2. Wersja Firmware
                stdin, stdout, stderr = ssh.exec_command("grep Arpversion /etc/plcnext/arpversion")
                fw_output = stdout.read().decode().strip()
                
                self.log(f"  🔍 Surowy output wersji firmware: '{fw_output}'")
                
                version_string = "?"
                if fw_output:
                    fw_output = fw_output.replace('Arpversion', '').strip()
                    
                    if ":" in fw_output:
                        parts = fw_output.split(':', 1)
                        version_string = parts[1].strip() if len(parts) > 1 else "?"
                    elif "=" in fw_output:
                        version_string = fw_output.split("=")[-1].strip()
                    else:
                        version_string = fw_output.strip()
                    
                    self.log(f"  🔍 Sparsowana wersja: '{version_string}'")
                
                if version_string and version_string != "?" and version_string[0].isdigit():
                    device.firmware_version = version_string
                else:
                    device.firmware_version = "?"
                    self.log(f"  ⚠️  Nie można odczytać poprawnej wersji firmware!")
                
                # 3. Strefa czasowa
                stdin, stdout, stderr = ssh.exec_command("cat /etc/timezone")
                device.timezone = stdout.read().decode(errors="ignore").strip()
                
                # 4. Sprawdzenie synchronizacji czasu
                plc_time_obj, plc_time_str, is_synced = self.check_time_sync(ssh)
                device.plc_time = plc_time_str
                device.time_sync_error = not is_synced
                
                # 5. System Services
                try:
                    remote_path = "/opt/plcnext/config/System/Scm/Default.scm.config"
                    remote_stat = sftp.stat(remote_path)
                    
                    local_file = resource_path(SYSTEM_SERVICES_FILE)
                    if os.path.exists(local_file):
                        local_size = os.path.getsize(local_file)
                        remote_size = remote_stat.st_size
                        
                        if local_size == remote_size:
                            device.system_services_ok = "OK"
                        else:
                            device.system_services_ok = "Różnica"
                            self.log(f"  ⚠️  System Services - różnica rozmiaru: lokalny={local_size}, zdalny={remote_size}")
                    else:
                        device.system_services_ok = "Istnieje"
                        
                except FileNotFoundError:
                    device.system_services_ok = "Brak"
                    self.log(f"  ⚠️  Plik System Services nie istnieje na sterowniku")
                except Exception as e:
                    device.system_services_ok = "Błąd"
                    self.log(f"  ⚠️  Błąd sprawdzania System Services: {str(e)}")
                
                # 6. Znacznik czasowy odczytu
                device.last_check = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Logowanie podsumowania
                self.log(f"  📦 Model: AXC F {device.plc_model}")
                self.log(f"  📦 Firmware: {device.firmware_version}")
                self.log(f"  🕐 Czas PLC: {device.plc_time}")
                self.log(f"  🌍 Strefa czasowa: {device.timezone}")
                self.log(f"  ⚙️  System Services: {device.system_services_ok}")
                
            # Context manager automatycznie zamknie SSH/SFTP tutaj
            
        except Exception as e:
            raise e




    def detect_plc_model(self, ssh):
        """
        Wykrywa model sterownika PLC za pomocą komendy 'rauc status'.
        Zwraca numer modelu (np. "2152", "3152", "1152") lub None w przypadku błędu.
        """
        try:
            stdin, stdout, stderr = ssh.exec_command("rauc status")
            rauc_output = stdout.read().decode(errors="ignore").strip()
            
            # Szukamy linii "Compatible: axcfXXXX_v1"
            for line in rauc_output.split('\n'):
                if 'Compatible:' in line:
                    # Przykład: "Compatible: axcf2152_v1"
                    parts = line.split(':')
                    if len(parts) > 1:
                        compatible = parts[1].strip()
                        # Wyciągamy numer modelu (2152, 3152, 1152)
                        if 'axcf' in compatible:
                            model = compatible.replace('axcf', '').split('_')[0]
                            self.log(f"  🔍 Wykryty model PLC: AXC F {model}")
                            return model
            
            self.log(f"  ⚠️  Nie można wykryć modelu z 'rauc status'")
            return None
            
        except Exception as e:
            self.log(f"  ⚠️  Błąd wykrywania modelu: {str(e)}")
            return None

    def extract_model_from_firmware(self, firmware_path):
        """
        Wyciąga numer modelu z nazwy pliku firmware.
        Przykład: 'axcf2152-2024.0.8_LTS-24.0.8.183.raucb' -> '2152'
        """
        filename = os.path.basename(firmware_path)
        if filename.startswith('axcf'):
            model = filename.split('-')[0].replace('axcf', '')
            return model
        return None

    def validate_firmware_compatibility(self, device, firmware_path):
        """
        Sprawdza czy firmware jest kompatybilny z modelem sterownika.
        Zwraca (True, message) jeśli kompatybilny, (False, message) jeśli nie.
        """
        fw_model = self.extract_model_from_firmware(firmware_path)
        
        if not fw_model:
            return False, "Nie można odczytać modelu z nazwy firmware"
        
        if not device.plc_model:
            return False, "Model sterownika nie został wykryty"
        
        if fw_model != device.plc_model:
            return False, f"NIEZGODNOŚĆ: Firmware dla {fw_model}, sterownik to {device.plc_model}"
        
        return True, f"Firmware kompatybilny z modelem {device.plc_model}"

    def check_time_sync(self, ssh):
        """
        Sprawdza czy czas sterownika jest zsynchronizowany z czasem systemowym.
        Zwraca (datetime_object, time_string, is_synced).
        """
        try:
            # Pobierz czas z sterownika z timeoutem
            stdin, stdout, stderr = ssh.exec_command("date '+%Y-%m-%d %H:%M:%S'", timeout=10)
            plc_time_str = stdout.read().decode(errors="ignore").strip()
            
            if not plc_time_str:
                self.log(f"  ⚠️ Nie można odczytać czasu ze sterownika")
                return None, "", False
            
            # Parsuj czas sterownika
            plc_time = datetime.strptime(plc_time_str, "%Y-%m-%d %H:%M:%S")
            
            # Pobierz aktualny czas lokalny (warszawski)
            local_tz = pytz.timezone(TIMEZONE)
            local_time = datetime.now(local_tz).replace(tzinfo=None)
            
            # Oblicz różnicę
            time_diff = abs((local_time - plc_time).total_seconds())
            
            # Tolerancja 60 sekund
            is_synced = time_diff < 60
            
            if not is_synced:
                self.log(f"  ⚠️ DESYNCHRONIZACJA CZASU: różnica {time_diff:.0f}s")
                self.log(f"    Sterownik: {plc_time_str}")
                self.log(f"    Lokalny: {local_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            return plc_time, plc_time_str, is_synced
            
        except Exception as e:
            self.log(f"  ⚠️ Błąd sprawdzania czasu: {str(e)}")
            return None, "", False

    def compare_firmware_versions(self, current_version, target_version):
        """
        Porównuje wersje firmware ze szczegółowym logowaniem.
        Zwraca True jeśli wersje są IDENTYCZNE (nie trzeba aktualizować).
        """
        target_version_number = self.get_target_fw_version(target_version)
        
        self.log(f"  🔍 Porównanie wersji firmware:")
        self.log(f"     Aktualna wersja na sterowniku: '{current_version}'")
        self.log(f"     Wersja z pliku firmware: '{target_version_number}'")
        
        if not current_version or current_version == "?":
            self.log(f"     ⚠️  Nie można odczytać aktualnej wersji - wymuszam aktualizację")
            return False 
        
        if not target_version_number:
            self.log(f"     ⚠️  Nie można odczytać wersji z pliku - wymuszam aktualizację")
            return False
        
        # Normalizacja: usuń białe znaki i porównaj
        current_clean = current_version.strip()
        target_clean = target_version_number.strip()
        
        is_same = current_clean == target_clean
        
        if is_same:
            self.log(f"     ✅ Wersje są IDENTYCZNE - aktualizacja NIE jest potrzebna")
        else:
            self.log(f"     ⚠️  Wersje są RÓŻNE - aktualizacja jest potrzebna")
            self.log(f"        Różnica: '{current_clean}' != '{target_clean}'")
        
        return is_same
    
    def get_target_fw_version(self, firmware_path):
        """Wyodrębnia numer wersji z nazwy pliku firmware."""
        # Przykład: 'axcf2152-2024.0.8_LTS-24.0.8.183.raucb' -> '24.0.8.183'
        filename = os.path.basename(firmware_path)
        
        # Usuń rozszerzenie .raucb
        if filename.endswith('.raucb'):
            filename = filename[:-6]
        
        # Podziel po myślniku
        parts = filename.split('-')
        
        # Ostatnia część to wersja (np. '24.0.8.183')
        if len(parts) >= 3:
            version = parts[-1]
            self.log(f"  🔍 Wykryta wersja firmware z pliku: {version}")
            return version
        
        self.log(f"  ⚠️  Nie można odczytać wersji z nazwy pliku: {filename}")
        return ""

    def create_widgets(self):
        """Tworzy interfejs użytkownika."""
        
        # Notebook (zakładki)
        notebook = ttk.Notebook(self)
        notebook.pack(fill="both", expand=True, padx=5, pady=5)
        
        # ZAKŁADKA 1: Przetwarzanie wsadowe
        batch_frame = tk.Frame(notebook)
        notebook.add(batch_frame, text="Przetwarzanie wsadowe")
        
        # Sekcja pliku Excel
        excel_frame = tk.LabelFrame(batch_frame, text="Plik Excel z listą sterowników", padx=10, pady=10)
        excel_frame.pack(fill="x", padx=10, pady=5)
        
        tk.Button(excel_frame, 
                  text="Wybierz plik Excel", 
                  command=self.select_excel, 
                  font=("Arial", 10, "bold")).pack(side="left", padx=5)

        tk.Label(excel_frame, textvariable=self.excel_path, bg="lightgray", relief="sunken", width=60).pack(side="left", padx=5)

        tk.Button(excel_frame,
                  text="Wczytaj listę",
                  command=self.load_excel,
                  font=("Arial", 10, "bold")).pack(side="left", padx=5)
        
        # Sekcja firmware
        firmware_frame = tk.LabelFrame(batch_frame, text="Plik Firmware (opcjonalnie dla aktualizacji)", padx=10, pady=10)
        firmware_frame.pack(fill="x", padx=10, pady=5)
        
        tk.Button(firmware_frame, 
                  text="Wybierz firmware",
                  command=self.select_firmware,
                  font=("Arial", 10, "bold")).pack(side="left", padx=5)

        tk.Label(firmware_frame, textvariable=self.firmware_path, bg="lightgray", relief="sunken", width=60).pack(side="left", padx=5)
        """
        # Typ sterownika
        plc_frame = tk.LabelFrame(batch_frame, text="Typ sterownika", padx=10, pady=5)
        plc_frame.pack(fill="x", padx=10, pady=5)
        tk.Radiobutton(plc_frame, text="AXC F 2152", variable=self.plc_type_var, value="2152").pack(side="left", padx=10)
        tk.Radiobutton(plc_frame, text="AXC F 3152", variable=self.plc_type_var, value="3152").pack(side="left", padx=10)
        """
        # Przyciski akcji - ODCZYT
        read_frame = tk.LabelFrame(batch_frame, text="Odczyt danych", padx=10, pady=5)
        read_frame.pack(fill="x", padx=10, pady=5)
        tk.Button(read_frame, text="Odczytaj wszystkie sterowniki", command=self.batch_read_all, 
        bg="#05DF72", fg="black", font=("Arial", 10, "bold")).pack(fill="x", padx=5, pady=4)

        # Przyciski akcji - AKTUALIZACJE (osobne)
        update_frame = tk.LabelFrame(batch_frame, text="Aktualizacje (wykonywane osobno)", padx=10, pady=5)
        update_frame.pack(fill="x", padx=10, pady=5)
        
        btn_grid = tk.Frame(update_frame)
        btn_grid.pack(fill="x", padx=5, pady=5)
        
        tk.Button(btn_grid, text="Wyślij System Services (wszystkie)", 
        command=self.batch_system_services, 
        bg="#A2F4FD", fg="black", font=("Arial", 10, "bold")).grid(row=0, column=0, padx=3, pady=2, sticky="ew")

        tk.Button(btn_grid, text="Ustaw strefę czasową (wszystkie)", 
            command=self.batch_timezone, 
            bg="#FFF085", fg="black", font=("Arial", 10, "bold")).grid(row=0, column=1, padx=3, pady=2, sticky="ew")

        tk.Button(btn_grid, text="Aktualizuj Firmware (wszystkie)", 
            command=self.batch_firmware_only, 
            bg="#BEDBFF", fg="black", font=("Arial", 10, "bold")).grid(row=1, column=0, padx=3, pady=2, sticky="ew")

        tk.Button(btn_grid, text="WYKONAJ WSZYSTKO NARAZ", 
            command=self.batch_update_all, 
            bg="#FFCCD3", fg="black", font=("Arial", 10, "bold")).grid(row=1, column=1, padx=3, pady=2, sticky="ew") # Zmieniono font na 10
        
        btn_grid.columnconfigure(0, weight=1)
        btn_grid.columnconfigure(1, weight=1)
        
        progress_frame = tk.LabelFrame(batch_frame, text="Status transferu plików", padx=10, pady=5)
        progress_frame.pack(fill="x", padx=10, pady=5)
        
        # Progress bar
        self.upload_progress = ttk.Progressbar(
            progress_frame, 
            orient="horizontal", 
            length=100, 
            mode="determinate"
        )
        self.upload_progress.pack(fill="x", padx=5, pady=5)
        
        # Label ze statusem
        self.upload_status_label = tk.Label(
            progress_frame, 
            text="Oczekiwanie na transfer...",
            font=("Arial", 9),
            fg="gray"
        )
        self.upload_status_label.pack(padx=5, pady=2)
    

        control_frame = tk.Frame(batch_frame)
        control_frame.pack(fill="x", padx=10, pady=5)

        tk.Button(control_frame, text="Zapisz raport Excel", command=self.save_excel, 
            bg="#2196F3", fg="black", font=("Arial", 10, "bold")).pack(side="left", padx=5, fill="x", expand=True)

        self.stop_btn = tk.Button(control_frame, text="STOP", command=self.stop_processing, 
            bg="#F44336", fg="black", font=("Arial", 10, "bold"), state="disabled")
        self.stop_btn.pack(side="left", padx=5, fill="x", expand=True)
        
        # Tabela ze sterownikami
        table_frame = tk.LabelFrame(batch_frame, text="Lista sterowników", padx=5, pady=5)
        table_frame.pack(fill="both", expand=True, padx=10, pady=5)

            # Scrollbar
        table_scroll_y = tk.Scrollbar(table_frame, orient="vertical")
        table_scroll_x = tk.Scrollbar(table_frame, orient="horizontal")

        self.device_tree = ttk.Treeview(table_frame, 
                                columns=("IP", "Model", "Firmware", "PLCTime", "Timezone", "SysServices", "LastCheck", "Status"),
                                show="tree headings",
                                yscrollcommand=table_scroll_y.set,
                                xscrollcommand=table_scroll_x.set)

        table_scroll_y.config(command=self.device_tree.yview)
        table_scroll_x.config(command=self.device_tree.xview)

        self.device_tree.heading("#0", text="Nazwa")
        self.device_tree.heading("IP", text="IP")
        self.device_tree.heading("Model", text="Model PLC")
        self.device_tree.heading("Firmware", text="Wersja Firmware")
        self.device_tree.heading("PLCTime", text="Czas sterownika")
        self.device_tree.heading("Timezone", text="Strefa czasowa")
        self.device_tree.heading("SysServices", text="System Services")
        self.device_tree.heading("LastCheck", text="Ostatni odczyt")
        self.device_tree.heading("Status", text="Status")

        self.device_tree.column("#0", width=150)
        self.device_tree.column("IP", width=120)
        self.device_tree.column("Model", width=80)
        self.device_tree.column("Firmware", width=150)
        self.device_tree.column("PLCTime", width=150)
        self.device_tree.column("Timezone", width=120)
        self.device_tree.column("SysServices", width=100)
        self.device_tree.column("LastCheck", width=150)
        self.device_tree.column("Status", width=120)

        # Konfiguracja tagów dla kolorowania
        self.device_tree.tag_configure('time_error', foreground='red')

        self.device_tree.pack(side="left", fill="both", expand=True)
        table_scroll_y.pack(side="right", fill="y")
        table_scroll_x.pack(side="bottom", fill="x")


        # ZAKŁADKA 2: Logi
        log_frame = tk.Frame(notebook)
        notebook.add(log_frame, text="Logi operacji")

        self.log_text = scrolledtext.ScrolledText(log_frame, wrap=tk.WORD, font=("Courier", 9))
        self.log_text.pack(fill="both", expand=True, padx=5, pady=5)

        tk.Button(log_frame, text="Wyczyść logi", command=self.clear_logs).pack(pady=5)

        # ZAKŁADKA 3: Ręczna obsługa (poprawiona)
        manual_frame = tk.Frame(notebook)
        notebook.add(manual_frame, text="Ręczna obsługa")
        self.create_manual_interface(manual_frame)

        # Status bar
        self.status_bar = tk.Label(self, text="Gotowy", relief="sunken", anchor="w", bg="lightgray")
        self.status_bar.pack(side="bottom", fill="x")

    def create_manual_interface(self, parent):
        """Tworzy interfejs do ręcznej obsługi pojedynczego sterownika."""
        
        connection_frame = tk.LabelFrame(parent, text="Połączenie", padx=10, pady=10)
        connection_frame.pack(fill="x", padx=10, pady=5)
        tk.Label(connection_frame, text="Adres IP:").pack()
        self.ip_entry = tk.Entry(connection_frame, width=20)
        self.ip_entry.pack()
        tk.Label(connection_frame, text="Hasło:").pack()
        self.password_entry = tk.Entry(connection_frame, show="*", width=20)
        self.password_entry.pack()
        
        # DODANE: Typ sterownika dla ręcznej obsługi
        tk.Label(connection_frame, text="Typ sterownika:").pack(pady=(10, 0))
        self.manual_plc_type_var = tk.StringVar(value="2152")
        plc_manual_frame = tk.Frame(connection_frame)
        plc_manual_frame.pack()
        tk.Radiobutton(plc_manual_frame, text="AXC F 2152", variable=self.manual_plc_type_var, value="2152").pack(side="left", padx=10)
        tk.Radiobutton(plc_manual_frame, text="AXC F 3152", variable=self.manual_plc_type_var, value="3152").pack(side="left", padx=10)
        
        tk.Button(connection_frame, text="Odczytaj dane z PLC", command=self.manual_read_plc).pack(pady=10)
        
        self.manual_data_label = tk.Label(parent, text="Tutaj pojawią się dane z PLC.",
                                         bg="lightyellow", relief="groove", justify="left",
                                         font=("Courier", 9), wraplength=450, padx=10, pady=10)
        self.manual_data_label.pack(fill="x", padx=10, pady=5)
        
        # Sekcja operacji ręcznych
        operations_frame = tk.LabelFrame(parent, text="Operacje pojedyncze", padx=10, pady=10)
        operations_frame.pack(fill="x", padx=10, pady=5)
        
        # Strefa czasowa
        tk.Button(operations_frame, text="🕐 Ustaw strefę czasową", 
                 command=self.manual_set_timezone, bg="#FF9800", fg="white",
                 font=("Arial", 10, "bold"), height=2).pack(fill="x", padx=5, pady=3)
        
        # System Services
        tk.Button(operations_frame, text="⚙️ Wyślij System Services", 
                 command=self.manual_upload_system_services, bg="#9C27B0", fg="white",
                 font=("Arial", 10, "bold"), height=2).pack(fill="x", padx=5, pady=3)
        
        # Firmware
        firmware_manual_frame = tk.LabelFrame(parent, text="Aktualizacja Firmware", padx=10, pady=10)
        firmware_manual_frame.pack(fill="x", padx=10, pady=5)
        
        tk.Button(firmware_manual_frame, text="Wybierz plik firmware", 
                 command=self.select_manual_firmware).pack(pady=5)
        self.manual_firmware_path = tk.StringVar()
        tk.Label(firmware_manual_frame, textvariable=self.manual_firmware_path, 
                bg="lightgray", relief="sunken", wraplength=400).pack(pady=5, fill="x")
        
        manual_fw_buttons = tk.Frame(firmware_manual_frame)
        manual_fw_buttons.pack(pady=5)
        tk.Button(manual_fw_buttons, text="📤 Wyślij firmware", 
                 command=self.manual_upload_firmware, bg="#4CAF50", fg="white",
                 font=("Arial", 10, "bold")).pack(side="left", padx=5)
        tk.Button(manual_fw_buttons, text="🔄 Wykonaj aktualizację", 
                 command=self.manual_execute_update, bg="#F44336", fg="white",
                 font=("Arial", 10, "bold")).pack(side="left", padx=5)

    def select_excel(self):
        """Wybór pliku Excel."""
        filepath = filedialog.askopenfilename(
            title="Wybierz plik Excel",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
        )
        if filepath:
            self.excel_path.set(filepath)

    def select_firmware(self):
        """Wybór pliku firmware."""
        filepath = filedialog.askopenfilename(title="Wybierz plik firmware")
        if filepath:
            self.firmware_path.set(filepath)

    def load_excel(self):
        """Wczytuje listę sterowników z pliku Excel."""
        excel_file = self.excel_path.get()
        if not excel_file or not os.path.exists(excel_file):
            messagebox.showerror("Błąd", "Wybierz prawidłowy plik Excel!")
            return
        
        try:
            wb = openpyxl.load_workbook(excel_file)
            ws = wb.active
            
            self.devices = []
            self.device_tree.delete(*self.device_tree.get_children())
            
            # Pomijamy nagłówek (wiersz 1)
            for row in ws.iter_rows(min_row=2, values_only=True):
                if row[0] and row[1]:  # Nazwa i IP muszą być wypełnione
                    name = str(row[0]).strip()
                    ip = str(row[1]).strip()
                    password = str(row[2]).strip() if row[2] else ""
                    
                    device = PLCDevice(name, ip, password)
                    
                    # Wczytaj istniejące dane jeśli są
                    if len(row) > 3 and row[3]:
                        device.firmware_version = str(row[3])
                    if len(row) > 4 and row[4]:
                        device.timezone = str(row[4])
                    if len(row) > 5 and row[5]:
                        device.system_services_ok = str(row[5])
                    if len(row) > 6 and row[6]:
                        device.last_check = str(row[6])
                    
                    self.devices.append(device)
                    self.device_tree.insert("", "end", text=name, values=(
                        ip, device.firmware_version, device.timezone, 
                        device.system_services_ok, device.last_check, device.status
                    ))
            
            wb.close()
            self.log(f"✓ Wczytano {len(self.devices)} sterowników z pliku Excel")
            messagebox.showinfo("Sukces", f"Wczytano {len(self.devices)} sterowników")
            
        except Exception as e:
            self.log(f"✗ Błąd wczytywania Excel: {str(e)}")
            messagebox.showerror("Błąd", f"Błąd wczytywania pliku Excel:\n{str(e)}")

    def save_excel(self):
        """Zapisuje aktualny stan do pliku Excel."""
        if not self.devices:
            messagebox.showwarning("Uwaga", "Brak danych do zapisania!")
            return
        
        try:
            save_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                initialfile=f"PLC_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            )
            
            if not save_path:
                return
            
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Sterowniki PLC"
            
            # Nagłówki
            headers = ["Nazwa Farmy", "IP", "Hasło", "Firmware", "Strefa czasowa", 
                      "System Services", "Ostatni odczyt", "Ostatnia aktualizacja", "Status", "Logi błędów"]
            ws.append(headers)
            
            # Formatowanie nagłówków
            header_fill = PatternFill(start_color="4CAF50", end_color="4CAF50", fill_type="solid")
            header_font = Font(bold=True, color="FFFFFF")
            for cell in ws[1]:
                cell.fill = header_fill
                cell.font = header_font
            
            # Dane
            for device in self.devices:
                ws.append([
                    device.name,
                    device.ip,
                    device.password,
                    device.firmware_version,
                    device.timezone,
                    device.system_services_ok,
                    device.last_check,
                    device.last_update,
                    device.status,
                    device.error_log
                ])
            
            # Dopasowanie szerokości kolumn
            for column in ws.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                ws.column_dimensions[column_letter].width = adjusted_width
            
            wb.save(save_path)
            self.log(f"✓ Zapisano raport do: {save_path}")
            messagebox.showinfo("Sukces", f"Raport zapisany:\n{save_path}")
            
        except Exception as e:
            self.log(f"✗ Błąd zapisu Excel: {str(e)}")
            messagebox.showerror("Błąd", f"Błąd zapisu do Excel:\n{str(e)}")

    def update_firmware_only_operation(self, device):
        """
        Aktualizuje TYLKO firmware (z automatycznym wykrywaniem modelu i walidacją).
        POPRAWIONA: Używa execute_firmware_update() dla bezpiecznego reebootu.
        """
        self.log(f"📦 Aktualizacja Firmware...")
        
        firmware_file = self.firmware_path.get()
        
        # Odczyt danych (w tym model PLC) - PRZERWIJ jeśli błąd
        try:
            self.read_single_device(device)
        except Exception as e:
            error_msg = f"Nie można odczytać danych sterownika przed aktualizacją: {str(e)}"
            self.log(f"  ❌ {error_msg}")
            raise Exception(error_msg)
        
        # Walidacja kompatybilności
        is_compatible, compat_msg = self.validate_firmware_compatibility(device, firmware_file)
        self.log(f"  🔍 {compat_msg}")
        
        if not is_compatible:
            raise Exception(compat_msg)
        
        # Sprawdź czy firmware jest aktualny
        if self.compare_firmware_versions(device.firmware_version, firmware_file):
            self.log(f"  ✅ Firmware już aktualny (v.{device.firmware_version}) - pomijam aktualizację")
            device.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return True
        
        try:
            # ✅ KROK 1: UPLOAD FIRMWARE (w context managerze)
            with self.ssh_connection(device) as (ssh, sftp):
                
                filename = os.path.basename(firmware_file)
                remote_fw_path = f"/opt/plcnext/{filename}"
                
                file_size = os.path.getsize(firmware_file)
                self.log(f"  📤 Wysyłanie firmware ({file_size/1024/1024:.1f} MB)...")
                
                sftp.put(
                    firmware_file, 
                    remote_fw_path,
                    callback=lambda transferred, total: self.upload_callback(
                        filename, transferred, total
                    )
                )
                
                self.reset_upload_progress()
                
                # Weryfikacja rozmiaru
                remote_size = sftp.stat(remote_fw_path).st_size
                if remote_size != file_size:
                    raise Exception(f"Transfer niepełny! Lokalny: {file_size}, Zdalny: {remote_size}")
                
                self.log(f"  ✓ Firmware wysłany i zweryfikowany")
            
            # ✅ Context manager zamknął SSH/SFTP tutaj
            
            # ✅ KROK 2: WYKONAJ UPDATE (NOWE połączenie SSH)
            self.execute_firmware_update(device)
            
            device.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return True
            
        except Exception as e:
            self.reset_upload_progress()
            raise e


    def batch_read_all(self):
            """Odczytuje dane ze wszystkich sterowników."""
            if not self.devices:
                messagebox.showwarning("Uwaga", "Najpierw wczytaj listę sterowników!")
                return
            
            if self.processing:
                messagebox.showwarning("Uwaga", "Operacja już w toku!")
                return
            
            threading.Thread(target=self.process_batch, args=("read",), daemon=True).start()

    def batch_system_services(self):
        """Wysyła System Services do wszystkich sterowników."""
        if not self.devices:
            messagebox.showwarning("Uwaga", "Najpierw wczytaj listę sterowników!")
            return
        
        if self.processing:
            messagebox.showwarning("Uwaga", "Operacja już w toku!")
            return
        
        response = messagebox.askyesno(
            "Potwierdzenie",
            f"Czy wysłać System Services do {len(self.devices)} sterowników?\n\n"
            "Każdy sterownik zostanie zrestartowany po aktualizacji."
        )
        
        if response:
            threading.Thread(target=self.process_batch, args=("system_services",), daemon=True).start()

    def batch_timezone(self):
        """Ustawia strefę czasową na wszystkich sterownikach."""
        if not self.devices:
            messagebox.showwarning("Uwaga", "Najpierw wczytaj listę sterowników!")
            return
        
        if self.processing:
            messagebox.showwarning("Uwaga", "Operacja już w toku!")
            return
        
        response = messagebox.askyesno(
            "Potwierdzenie",
            f"Czy ustawić strefę czasową {TIMEZONE} na {len(self.devices)} sterownikach?\n\n"
            "Każdy sterownik zostanie zrestartowany."
        )
        
        if response:
            threading.Thread(target=self.process_batch, args=("timezone",), daemon=True).start()

    def batch_firmware_only(self):
        """Aktualizuje firmware na wszystkich sterownikach."""
        if not self.devices:
            messagebox.showwarning("Uwaga", "Najpierw wczytaj listę sterowników!")
            return
        
        if self.processing:
            messagebox.showwarning("Uwaga", "Operacja już w toku!")
            return
        
        firmware_file = self.firmware_path.get()
        if not firmware_file or not os.path.exists(firmware_file):
            messagebox.showerror("Błąd", "Wybierz prawidłowy plik firmware!")
            return
        
        response = messagebox.askyesno(
            "Potwierdzenie",
            f"Czy zaktualizować firmware na {len(self.devices)} sterownikach?\n\n"
            "Każdy sterownik zostanie zrestartowany po aktualizacji.\n"
            "To może zająć dużo czasu!"
        )
        
        if response:
            threading.Thread(target=self.process_batch, args=("firmware",), daemon=True).start()

    def batch_update_all(self):
        """WYKONUJE WSZYSTKIE OPERACJE NARAZ - zoptymalizowane."""
        if not self.devices:
            messagebox.showwarning("Uwaga", "Najpierw wczytaj listę sterowników!")
            return
        
        if self.processing:
            messagebox.showwarning("Uwaga", "Operacja już w toku!")
            return
        
        firmware_file = self.firmware_path.get()
        if not firmware_file or not os.path.exists(firmware_file):
            messagebox.showerror("Błąd", "Wybierz prawidłowy plik firmware!")
            return
        
        response = messagebox.askyesno(
            "Potwierdzenie",
            f"🚀 PEŁNA AKTUALIZACJA {len(self.devices)} sterowników:\n\n"
            "Operacje wykonywane dla każdego sterownika:\n"
            "1. System Services (jeśli potrzebne)\n"
            "2. Firmware - wysłanie i sudo update\n"
            "3. Strefa czasowa (jeśli potrzebne)\n"
            "4. Restart sterownika\n\n"
            "Operacja może zająć bardzo dużo czasu!\n\n"
            "Kontynuować?"
        )
        
        if response:
            threading.Thread(target=self.process_batch, args=("all",), daemon=True).start()


    def update_system_services_only(self, device):
        """
        Wysyła System Services i restartuje sterownik. Pomija, jeśli jest już OK.
        POPRAWIONA: Używa execute_reboot() dla bezpiecznego reebootu.
        """
        self.log(f"⚙️  Aktualizacja System Services...")
        
        # Sprawdzenie statusu przed operacją
        try:
            self.read_single_device(device)
        except Exception as e:
            self.log(f"  ⚠️  Błąd odczytu przed aktualizacją SysServices: {str(e)}")
        
        # Logika pominięcia
        if device.system_services_ok == "OK":
            self.log(f"  ℹ️  System Services już aktualne - pomijam")
            device.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return True
        
        try:
            # ✅ KROK 1: UPLOAD SYSTEM SERVICES (w context managerze)
            with self.ssh_connection(device) as (ssh, sftp):
                
                local_sys_file = resource_path(SYSTEM_SERVICES_FILE)
                if not os.path.exists(local_sys_file):
                    raise Exception(f"Plik {SYSTEM_SERVICES_FILE} nie istnieje!")
                
                remote_sys_path = "/opt/plcnext/config/System/Scm/Default.scm.config"
                filename = os.path.basename(local_sys_file)
                file_size = os.path.getsize(local_sys_file)
                
                self.log(f"  📤 Wysyłanie {filename} ({file_size/1024:.1f} KB)...")
                
                sftp.put(
                    local_sys_file, 
                    remote_sys_path,
                    callback=lambda transferred, total: self.upload_callback(
                        filename, transferred, total
                    )
                )
                
                self.reset_upload_progress()
                
                # Weryfikacja
                remote_size = sftp.stat(remote_sys_path).st_size
                if file_size != remote_size:
                    raise Exception(f"Transfer niepełny! Lokalny: {file_size}, Zdalny: {remote_size}")
                
                device.system_services_ok = "OK"
                self.log(f"  ✓ System Services wysłane i zweryfikowane")
            
            # ✅ Context manager zamknął SSH/SFTP tutaj
            
            # ✅ KROK 2: REBOOT (NOWE połączenie SSH)
            self.execute_reboot(device)
            
            device.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return True
            
        except Exception as e:
            self.reset_upload_progress()
            raise e


    def update_timezone_only(self, device):
        """
        Ustawia strefę czasową i restartuje. Pomija, jeśli już OK.
        POPRAWIONA: Używa execute_reboot() dla bezpiecznego reebootu.
        """
        self.log(f"🕐 Aktualizacja strefy czasowej na {TIMEZONE}...")
        
        # Sprawdzenie statusu przed operacją
        try:
            self.read_single_device(device)
        except Exception as e:
            self.log(f"  ⚠️  Błąd odczytu przed aktualizacją Timezone: {str(e)}")
        
        # Logika pominięcia
        if device.timezone.strip() == TIMEZONE.strip():
            self.log(f"  ℹ️  Strefa czasowa już ustawiona na {TIMEZONE} - pomijam")
            device.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return True
        
        try:
            # ✅ KROK 1: USTAWIENIE TIMEZONE (w context managerze)
            with self.ssh_connection(device) as (ssh, sftp):
                
                self.log(f"  📝 Ustawianie strefy czasowej na {TIMEZONE}...")
                
                # Wpisanie TIMEZONE do /etc/timezone
                stdin, stdout, stderr = ssh.exec_command(
                    f"sudo sh -c 'echo {TIMEZONE} > /etc/timezone'", 
                    get_pty=True
                )
                stdin.write(device.password + "\n")
                stdin.flush()
                time.sleep(1)
                
                # Użycie timedatectl
                stdin, stdout, stderr = ssh.exec_command(
                    f"sudo timedatectl set-timezone {TIMEZONE}", 
                    get_pty=True
                )
                stdin.write(device.password + "\n")
                stdin.flush()
                time.sleep(1)
                
                device.timezone = TIMEZONE
                self.log("  ✓ Strefa czasowa ustawiona")
            
            # ✅ Context manager zamknął SSH/SFTP tutaj
            
            # ✅ KROK 2: REBOOT (NOWE połączenie SSH)
            self.execute_reboot(device)
            
            device.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return True
            
        except Exception as e:
            raise e

    def update_all_operations(self, device):
        """
        Wykonuje wszystkie operacje: System Services, Firmware, Timezone.
        Zoptymalizowane pod kątem restartów i pomijania.
        
        KOLEJNOŚĆ OPERACJI:
        1. Połączenie SSH (przez context manager)
        2. Wykrycie modelu PLC i walidacja kompatybilności firmware
        3. Odczyt wstępny (stan SysServices, Timezone, Firmware)
        4. Aktualizacja System Services (tylko jeśli jest różnica/brak)
        5. Aktualizacja Firmware (tylko wysłanie pliku - jeśli konieczne i kompatybilne)
        6. Ustawienie strefy czasowej (tylko jeśli konieczne)
        7. ZAMKNIĘCIE SFTP przed rebootem/update
        8. Wykonanie sudo update / sudo reboot (tylko jeśli FW lub SS było wgrywane)
        """
        self.log(f"🚀 PEŁNA AKTUALIZACJA: START")
        
        firmware_file = self.firmware_path.get()
        
        # Flagi kontrolujące potrzebę restartu/update
        ss_updated = False
        fw_needed = False
        tz_updated = False
        
        try:
            # ✅ UŻYJ CONTEXT MANAGERA dla bezpiecznego SSH/SFTP
            with self.ssh_connection(device) as (ssh, sftp):
                
                # 1. Wykryj model PLC
                device.plc_model = self.detect_plc_model(ssh)
                
                if not device.plc_model:
                    raise Exception("❌ Nie można wykryć modelu sterownika!")
                
                # 2. Walidacja kompatybilności firmware
                is_compatible, compat_msg = self.validate_firmware_compatibility(device, firmware_file)
                self.log(f"  🔍 {compat_msg}")
                
                if not is_compatible:
                    raise Exception(f"❌ {compat_msg}\n\n⚠️ ZATRZYMANO AKTUALIZACJĘ!")
                
                # 3. Odczyt wstępny danych
                self.log("  📖 Wstępny odczyt danych...")
                
                # Firmware version
                stdin, stdout, stderr = ssh.exec_command("grep Arpversion /etc/plcnext/arpversion")
                fw_output = stdout.read().decode().strip()
                
                self.log(f"  🔍 Surowy output wersji firmware: '{fw_output}'")
                
                version_string = "?"
                if fw_output:
                    fw_output = fw_output.replace('Arpversion', '').strip()
                    
                    if ":" in fw_output:
                        parts = fw_output.split(':', 1) 
                        version_string = parts[1].strip() if len(parts) > 1 else "?"
                    elif "=" in fw_output:
                        version_string = fw_output.split("=")[-1].strip()
                    else:
                        version_string = fw_output.strip()
                    
                    self.log(f"  🔍 Sparsowana wersja: '{version_string}'")
                
                if version_string and version_string != "?" and version_string[0].isdigit():
                    device.firmware_version = version_string
                else:
                    device.firmware_version = "?"
                    self.log(f"  ⚠️ Nie można odczytać poprawnej wersji firmware!")
                
                # Timezone
                stdin, stdout, stderr = ssh.exec_command("cat /etc/timezone")
                device.timezone = stdout.read().decode(errors="ignore").strip()
                
                # System Services
                try:
                    remote_path = "/opt/plcnext/config/System/Scm/Default.scm.config"
                    remote_stat = sftp.stat(remote_path)
                    local_file = resource_path(SYSTEM_SERVICES_FILE)
                    if os.path.exists(local_file):
                        local_size = os.path.getsize(local_file)
                        remote_size = remote_stat.st_size
                        device.system_services_ok = "OK" if local_size == remote_size else "Różnica"
                        if device.system_services_ok == "Różnica":
                            self.log(f"  ⚠️ System Services - różnica rozmiaru: lokalny={local_size}, zdalny={remote_size}")
                    else:
                        device.system_services_ok = "Istnieje"
                except FileNotFoundError:
                    device.system_services_ok = "Brak"
                except Exception as e:
                    device.system_services_ok = "Błąd"
                    self.log(f"  ⚠️ Błąd sprawdzania System Services: {str(e)}")
                
                self.log(f"  ⚙️ Status System Services: {device.system_services_ok}")
                self.log(f"  📦 Aktualna wersja FW: {device.firmware_version}")
                self.log(f"  🕐 Aktualna strefa czasowa: {device.timezone}")
                
                # 4. System Services - TYLKO UPLOAD, REBOOT PÓŹNIEJ
                if device.system_services_ok != "OK":
                    self.log(f"  ⚙️ System Services: {device.system_services_ok}. Wymagana aktualizacja.")
                    
                    local_sys_file = resource_path(SYSTEM_SERVICES_FILE)
                    if not os.path.exists(local_sys_file):
                        raise Exception(f"Plik {SYSTEM_SERVICES_FILE} nie istnieje lokalnie!")
                    
                    remote_sys_path = "/opt/plcnext/config/System/Scm/Default.scm.config"
                    filename = os.path.basename(local_sys_file)
                    
                    self.log(f"  📤 Wysyłanie {filename}...")
                    
                    sftp.put(
                        local_sys_file, 
                        remote_sys_path,
                        callback=lambda transferred, total: self.upload_callback(
                            filename, transferred, total
                        )
                    )
                    
                    # Weryfikacja
                    remote_size = sftp.stat(remote_sys_path).st_size
                    local_size = os.path.getsize(local_sys_file)
                    if remote_size != local_size:
                        raise Exception(f"Transfer SS niepełny! Lokalny: {local_size}, Zdalny: {remote_size}")
                    
                    self.reset_upload_progress()
                    device.system_services_ok = "OK"
                    ss_updated = True
                    self.log(f"  ✓ System Services wysłane i zweryfikowane")
                else:
                    self.log("  ⚙️ System Services OK - pomijam wysyłkę")
                
                # 5. Firmware - TYLKO UPLOAD, UPDATE PÓŹNIEJ
                if not self.compare_firmware_versions(device.firmware_version, firmware_file):
                    fw_needed = True
                    target_fw_version = self.get_target_fw_version(firmware_file)
                    self.log(f"  📦 Firmware nieaktualne. Aktualna: {device.firmware_version}, Docelowa: {target_fw_version}")
                    
                    self.log("  📤 Wysyłanie Firmware...")
                    filename = os.path.basename(firmware_file)
                    remote_fw_path = f"/opt/plcnext/{filename}"
                    
                    file_size = os.path.getsize(firmware_file)
                    
                    sftp.put(
                        firmware_file, 
                        remote_fw_path,
                        callback=lambda transferred, total: self.upload_callback(
                            filename, transferred, total
                        )
                    )
                    
                    # Weryfikacja
                    remote_size = sftp.stat(remote_fw_path).st_size
                    if remote_size != file_size:
                        raise Exception(f"Transfer FW niepełny! Lokalny: {file_size}, Zdalny: {remote_size}")
                    
                    self.reset_upload_progress()
                    self.log(f"  ✓ Plik firmware wysłany i zweryfikowany ({file_size/1024/1024:.1f} MB)")
                else:
                    self.log(f"  ✅ Firmware (v.{device.firmware_version}) jest aktualne - pomijam wysyłkę")

                # 6. Timezone - TYLKO USTAWIENIE, REBOOT PÓŹNIEJ
                if device.timezone.strip() != TIMEZONE.strip():
                    self.log(f"  🕐 Strefa czasowa niepoprawna. Ustawianie na {TIMEZONE}...")
                    
                    stdin, stdout, stderr = ssh.exec_command(
                        f"sudo sh -c 'echo {TIMEZONE} > /etc/timezone'", 
                        get_pty=True
                    )
                    stdin.write(device.password + "\n")
                    stdin.flush()
                    time.sleep(1)
                    
                    stdin, stdout, stderr = ssh.exec_command(
                        f"sudo timedatectl set-timezone {TIMEZONE}", 
                        get_pty=True
                    )
                    stdin.write(device.password + "\n")
                    stdin.flush()
                    time.sleep(1)
                    
                    device.timezone = TIMEZONE
                    tz_updated = True
                    self.log("  ✓ Strefa czasowa ustawiona")
                else:
                    self.log("  🕐 Strefa czasowa OK - pomijam zmianę")
                
                # ✅ KLUCZOWE: Zamknij SFTP PRZED jakimkolwiek rebootem/update
                self.log("  🔒 Zamykam SFTP przed rebootem/update...")
                sftp.close()
            
            # ✅ Context manager zamknął SSH tutaj - wszystkie transfery zakończone!
            
            # 7. TERAZ WYKONAJ UPDATE/REBOOT (nowe połączenie SSH)
            needs_reboot = ss_updated or tz_updated
            
            if fw_needed or needs_reboot:
                self.log("  🔄 WYKONYWANIE AKTUALIZACJI / RESTART...")
                
                if fw_needed:
                    # Firmware update - to robi automatyczny reboot
                    self.execute_firmware_update(device)
                    
                elif needs_reboot:
                    # Tylko reboot (SS lub TZ się zmieniły, ale nie FW)
                    self.execute_reboot(device)
            else:
                self.log("  ℹ️ Wszystkie komponenty aktualne. Pomijam restart")

            device.last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return True
            
        except Exception as e:
            self.reset_upload_progress()
            raise e


    def set_timezone_ssh(self, ssh, password):
        """
        Ustawia strefę czasową przez SSH (bez restartu).
        Używa shell interaktywnego z sudo i su.
        """
        shell = ssh.invoke_shell()
        
        def send_cmd(cmd, wait=1):
            shell.send(cmd + "\n")
            time.sleep(wait)
        
        # Ustaw hasło root
        send_cmd("sudo passwd root")
        send_cmd(password)  # sudo password
        send_cmd(ROOT_PASS)  # nowe hasło root
        send_cmd(ROOT_PASS)  # potwierdzenie
        
        # Przełącz na root
        send_cmd("su")
        send_cmd(ROOT_PASS)
        
        # Ustaw strefę czasową
        send_cmd(f"ln -sf /usr/share/zoneinfo/{TIMEZONE} /etc/localtime")
        send_cmd(f"echo '{TIMEZONE}' > /etc/timezone")
        
        # Wyłącz hasło root
        send_cmd("passwd -dl root")
        send_cmd("exit")
        
        time.sleep(2)

    def update_device_row(self, device):
        """Aktualizuje pojedynczy wiersz w Treeview z kolorowaniem czasu."""
        
        item_id = None
        for item in self.device_tree.get_children():
            if self.device_tree.item(item, 'text') == device.name:
                item_id = item
                break
        
        if item_id:
            # Aktualizuj wartości
            self.device_tree.item(item_id, values=(
                device.ip,
                f"AXC F {device.plc_model}" if device.plc_model else "?",
                device.firmware_version,
                device.plc_time, 
                device.timezone, 
                device.system_services_ok, 
                device.last_check, 
                device.status
            ))
            
            # NOWE: Ustaw tag dla kolorowania jeśli jest błąd synchronizacji czasu
            if device.time_sync_error:
                self.device_tree.item(item_id, tags=('time_error',))
            else:
                self.device_tree.item(item_id, tags=())
            
            self.device_tree.update_idletasks()

    def stop_processing(self):
        """Zatrzymuje przetwarzanie."""
        if messagebox.askyesno("Potwierdzenie", "Czy na pewno chcesz zatrzymać operację?"):
            self.processing = False
            self.log("⏹️  Żądanie zatrzymania operacji...")

    def log(self, message):
        """Dodaje wiadomość do kolejki logów."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_queue.put(f"[{timestamp}] {message}")

    def update_logs(self):
        """Aktualizuje okno logów z kolejki."""
        try:
            while True:
                message = self.log_queue.get_nowait()
                self.log_text.insert(tk.END, message + "\n")
                self.log_text.see(tk.END)
        except queue.Empty:
            pass
        finally:
            self.after(100, self.update_logs)

    def clear_logs(self):
        """Czyści okno logów."""
        self.log_text.delete(1.0, tk.END)

    # ============================================================================
    # RĘCZNA OBSŁUGA - pojedyncze operacje
    # ============================================================================

    def manual_read_plc(self):
        """Ręczny odczyt pojedynczego PLC."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        if not ip or not password:
            messagebox.showerror("Błąd", "Podaj IP i hasło!")
            return
        
        device = PLCDevice("Manual", ip, password)
        threading.Thread(target=self.manual_read_worker, args=(device,), daemon=True).start()

    def manual_read_worker(self, device):
        """Worker dla ręcznego odczytu."""
        try:
            self.status_bar.config(text="Łączenie z PLC...")
            self.read_single_device(device)
            
            display_text = (
                f"Adres IP: {device.ip}\n"
                f"Aktualny czas: {device.last_check}\n"
                f"Strefa czasowa: {device.timezone}\n\n"
                f"Wersja Firmware: {device.firmware_version}\n\n"
                f"System Services: {device.system_services_ok}"
            )
            
            self.manual_data_label.config(text=display_text)
            self.status_bar.config(text="Gotowy")
            self.log(f"✓ Odczytano dane z {device.ip}")
            
        except Exception as e:
            self.status_bar.config(text="Błąd")
            self.manual_data_label.config(text=f"Błąd odczytu:\n{str(e)}")
            self.log(f"✗ Błąd odczytu z {device.ip}: {str(e)}")
            messagebox.showerror("Błąd", f"Błąd odczytu:\n{str(e)}")

    def select_manual_firmware(self):
        """Wybór pliku firmware dla ręcznej obsługi."""
        filepath = filedialog.askopenfilename(title="Wybierz plik firmware")
        if filepath:
            self.manual_firmware_path.set(filepath)

    def manual_set_timezone(self):
        """Ręczne ustawienie strefy czasowej."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        if not ip or not password:
            messagebox.showerror("Błąd", "Podaj IP i hasło!")
            return
        
        response = messagebox.askyesno(
            "Potwierdzenie",
            f"Czy ustawić strefę czasową na {TIMEZONE}?\n"
            "Sterownik zostanie zrestartowany!"
        )
        if not response:
            return
        
        device = PLCDevice("Manual", ip, password)
        threading.Thread(target=self.manual_timezone_worker, args=(device,), daemon=True).start()

    def manual_timezone_worker(self, device):
        """Worker dla ustawiania strefy czasowej."""
        try:
            self.status_bar.config(text="Ustawianie strefy czasowej...")
            self.update_timezone_only(device)
            
            self.status_bar.config(text="Gotowy")
            self.after(0, lambda: messagebox.showinfo(
                "Sukces",
                f"Strefa czasowa została zmieniona na {TIMEZONE}\n"
                "Sterownik został zrestartowany."
            ))
            
        except Exception as e:
            self.status_bar.config(text="Błąd")
            self.log(f"✗ Błąd ustawiania strefy czasowej: {str(e)}")
            self.after(0, lambda: messagebox.showerror("Błąd", f"Błąd:\n{str(e)}"))

    def manual_upload_system_services(self):
        """Ręczne wysłanie System Services."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        if not ip or not password:
            messagebox.showerror("Błąd", "Podaj IP i hasło!")
            return
        
        local_file = resource_path(SYSTEM_SERVICES_FILE)
        if not os.path.exists(local_file):
            messagebox.showerror("Błąd", f"Plik {SYSTEM_SERVICES_FILE} nie istnieje!")
            return
        
        response = messagebox.askyesno(
            "Potwierdzenie",
            "Czy wysłać plik System Services?\n"
            "Sterownik zostanie zrestartowany!"
        )
        if not response:
            return
        
        device = PLCDevice("Manual", ip, password)
        threading.Thread(target=self.manual_sys_services_worker, args=(device,), daemon=True).start()

    def manual_sys_services_worker(self, device):
        """Worker dla wysyłania System Services."""
        try:
            self.status_bar.config(text="Wysyłanie System Services...")
            self.update_system_services_only(device)
            
            self.status_bar.config(text="Gotowy")
            self.after(0, lambda: messagebox.showinfo(
                "Sukces",
                "Plik System Services został przesłany!\n"
                "Sterownik został zrestartowany."
            ))
            
        except Exception as e:
            self.status_bar.config(text="Błąd")
            self.log(f"✗ Błąd wysyłania System Services: {str(e)}")
            self.after(0, lambda: messagebox.showerror("Błąd", f"Błąd:\n{str(e)}"))

    def manual_upload_firmware(self):
        """Ręczne wysłanie firmware (bez wykonania update)."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        firmware_file = self.manual_firmware_path.get()
        
        if not ip or not password:
            messagebox.showerror("Błąd", "Podaj IP i hasło!")
            return
        
        if not firmware_file or not os.path.exists(firmware_file):
            messagebox.showerror("Błąd", "Wybierz prawidłowy plik firmware!")
            return
        
        threading.Thread(target=self.manual_upload_fw_worker, 
                        args=(ip, password, firmware_file), daemon=True).start()

    def manual_upload_fw_worker(self, ip, password, firmware_file):
        """Worker dla wysyłania firmware."""
        ssh = None
        sftp = None
        try:
            self.status_bar.config(text="Wysyłanie firmware...")
            self.log(f"Łączenie z {ip} - wysyłanie firmware...")
            
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username=PLC_USER, password=password, timeout=30)
            
            sftp = ssh.open_sftp()
            filename = os.path.basename(firmware_file)
            remote_path = f"/opt/plcnext/{filename}"
            
            file_size = os.path.getsize(firmware_file)
            self.log(f"Wysyłanie {filename} ({file_size/1024/1024:.1f} MB)...")
            
            sftp.put(firmware_file, remote_path)
            
            # Weryfikacja
            remote_size = sftp.stat(remote_path).st_size
            sftp.close()
            ssh.close()
            
            if remote_size == file_size:
                self.status_bar.config(text="Gotowy")
                self.log(f"✓ Firmware przesłane pomyślnie")
                self.after(0, lambda: messagebox.showinfo(
                    "Sukces",
                    f"Firmware zostało przesłane!\n"
                    f"Ścieżka: {remote_path}\n"
                    f"Rozmiar: {remote_size/1024/1024:.1f} MB\n\n"
                    f"Użyj przycisku 'Wykonaj aktualizację' aby zainstalować."
                ))
            else:
                raise Exception(f"Transfer niepełny! Oczekiwano {file_size}, otrzymano {remote_size}")
            
        except Exception as e:
            if sftp:
                sftp.close()
            if ssh:
                ssh.close()
            self.status_bar.config(text="Błąd")
            self.log(f"✗ Błąd wysyłania firmware: {str(e)}")
            self.after(0, lambda: messagebox.showerror("Błąd", f"Błąd:\n{str(e)}"))

    def manual_execute_update(self):
        """Ręczne wykonanie aktualizacji firmware."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        
        if not ip or not password:
            messagebox.showerror("Błąd", "Podaj IP i hasło!")
            return
        
        plc_type = self.manual_plc_type_var.get()
        response = messagebox.askyesno(
            "Potwierdzenie",
            f"Czy wykonać aktualizację firmware?\n"
            f"Komenda: sudo update-axcf{plc_type}\n\n"
            "Sterownik zostanie zrestartowany!"
        )
        if not response:
            return
        
        threading.Thread(target=self.manual_execute_update_worker, 
                        args=(ip, password, plc_type), daemon=True).start()

    def manual_execute_update_worker(self, ip, password, plc_type):
        """Worker dla wykonania aktualizacji."""
        try:
            self.status_bar.config(text="Wykonywanie aktualizacji...")
            self.log(f"Łączenie z {ip} - wykonywanie aktualizacji firmware...")
            
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username=PLC_USER, password=password, timeout=30)
            
            self.log(f"Wykonywanie: sudo update-axcf{plc_type}")
            stdin, stdout, stderr = ssh.exec_command(f"sudo update-axcf{plc_type}", get_pty=True)
            stdin.write(password + "\n")
            stdin.flush()
            
            output = ""
            while True:
                if stdout.channel.recv_ready():
                    chunk = stdout.read(1024).decode(errors="ignore")
                    output += chunk
                if stdout.channel.exit_status_ready():
                    break
                time.sleep(0.5)
            
            errors = stderr.read().decode(errors="ignore")
            
            ssh.close()
            
            if "error" in output.lower() or "failed" in output.lower() or errors.strip():
                raise Exception(f"Update zwrócił błąd:\n{output}\n{errors}")
            
            self.status_bar.config(text="Gotowy")
            self.log(f"✓ Aktualizacja zakończona - sterownik restartuje się")
            self.after(0, lambda: messagebox.showinfo(
                "Sukces",
                "Aktualizacja firmware zakończona!\n"
                "Sterownik został zrestartowany.\n\n"
                f"Output:\n{output[:300]}..."
            ))
            
        except Exception as e:
            self.status_bar.config(text="Błąd")
            self.log(f"✗ Błąd aktualizacji: {str(e)}")
            self.after(0, lambda: messagebox.showerror("Błąd", f"Błąd:\n{str(e)}"))


if __name__ == "__main__":
    app = BatchProcessorApp()
    app.mainloop()