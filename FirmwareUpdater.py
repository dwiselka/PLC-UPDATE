import paramiko
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading
import os
import time
from datetime import datetime
import pytz
import sys

# cd "C:\Users\dawid.wiselka\OneDrive - NOMAD ELECTRIC Sp. z o.o\Dokumenty\Farmy\Updater\all"
# pyinstaller --onefile --noconsole --icon="plcv2.ico" --add-data "plcv2.ico;." "FirmwareUpdater.py"
# python FirmwareUpdater.py
# pyinstaller --onefile --noconsole --icon=plcv2.ico --add-data "plcv2.ico;." --add-data "Default.scm.config;." FirmwareUpdater.py

PLC_USER = "admin"
ROOT_PASS = "12345"
TIMEZONE = "Europe/Warsaw"

def resource_path(relative_path):
    """Zwraca absolutną ścieżkę do pliku, działa również w exe PyInstaller."""
    try:
        # PyInstaller tworzy tymczasowy folder _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

class FirmwareUpdaterApp(tk.Tk):
    """Główna klasa aplikacji Tkinter do aktualizacji firmware PLC."""

    def __init__(self):
        super().__init__()
        self.title("Narzędzia PLC Phoenix Contact")
        self.geometry("500x850")
        self.iconbitmap(resource_path("plcv2.ico"))

        # Zmienne stanu aplikacji
        self.firmware_path = tk.StringVar()
        self.progress_var = tk.DoubleVar()
        self.plc_type_var = tk.StringVar(value="2152")
        self.speed_var = tk.StringVar(value="Prędkość: -- KB/s")

        # Zmienne do śledzenia prędkości SFTP
        self.last_transferred = 0
        self.last_time = time.time()

        # Tworzenie GUI
        self.create_widgets()

    def create_widgets(self):
        """Tworzy i rozmieszcza wszystkie elementy GUI."""
        # Ramka dla połączenia
        connection_frame = tk.LabelFrame(self, text="Połączenie", padx=10, pady=10)
        connection_frame.pack(fill="x", padx=10, pady=5)
        tk.Label(connection_frame, text="Adres IP:").pack()
        self.ip_entry = tk.Entry(connection_frame, width=20)
        self.ip_entry.pack()
        tk.Label(connection_frame, text="Hasło:").pack()
        self.password_entry = tk.Entry(connection_frame, show="*", width=20)
        self.password_entry.pack()

        connection_buttons_frame = tk.Frame(connection_frame)
        connection_buttons_frame.pack(pady=15)
        #tk.Button(connection_buttons_frame, text="Testuj połączenie", command=self.test_connection).pack(side="left", padx=5)
        tk.Button(connection_buttons_frame, text="Odczytaj dane z PLC", command=self.read_plc_data).pack(side="left", padx=10)

        # Ramka dla typu sterownika
        plc_frame = tk.LabelFrame(self, text="Typ sterownika", padx=10, pady=10)
        plc_frame.pack(fill="x", padx=10, pady=5)
        tk.Radiobutton(plc_frame, text="AXC F 2152", variable=self.plc_type_var, value="2152").pack(anchor="w")
        tk.Radiobutton(plc_frame, text="AXC F 3152", variable=self.plc_type_var, value="3152").pack(anchor="w")

        # Ramka dla zarządzania czasem
        time_frame = tk.LabelFrame(self, text="Zarządzanie", padx=10, pady=5)
        time_frame.pack(fill="x", padx=10, pady=5)
        time_buttons_frame = tk.Frame(time_frame)
        time_buttons_frame.pack()
        tk.Button(time_buttons_frame, text="Ustaw strefę czasową", command=self.set_timezone).pack(side="left", padx=5)
        tk.Button(time_buttons_frame, text=" Ustaw System Services", command=self.upload_system_services).pack(side="left", padx=5)

        # Ramka dla firmware
        firmware_frame = tk.LabelFrame(self, text="Firmware", padx=10, pady=10)
        firmware_frame.pack(fill="x", padx=10, pady=5)
        tk.Button(firmware_frame, text="Wybierz plik firmware", command=self.select_file).pack(pady=5)
        tk.Label(firmware_frame, textvariable=self.firmware_path, wraplength=450,
                  bg="lightgray", relief="sunken").pack(pady=5, fill="x")

        # Przyciski firmware
        firmware_buttons_frame = tk.Frame(firmware_frame)
        firmware_buttons_frame.pack(pady=10)
        tk.Button(firmware_buttons_frame, text="Wyślij plik", command=self.upload_firmware).pack(side="left", padx=5)
        tk.Button(firmware_buttons_frame, text="Wykonaj aktualizację", command=self.execute_firmware_update).pack(side="left", padx=5)

        # Status i postęp
        self.status_label = tk.Label(self, text="Gotowy", fg="blue", font=("Arial", 10, "bold"))
        self.status_label.pack(pady=5)

        self.progress_bar = ttk.Progressbar(self, variable=self.progress_var, maximum=100, length=450)
        self.progress_bar.pack(pady=5)

        # Etykieta do wyświetlania postępu i prędkości
        self.info_label = tk.Label(self, text="", fg="black", font=("Arial", 9))
        self.info_label.pack()
        
        # Etykieta do wyświetlania prędkości
        self.speed_label = tk.Label(self, textvariable=self.speed_var, fg="black", font=("Arial", 9))
        self.speed_label.pack()

        # Etykieta do wyświetlania danych z PLC
        self.data_label = tk.Label(self, text="Tutaj pojawią się dane z PLC (czas i wersja firmware).",
                                  bg="lightyellow", relief="groove", justify="left",
                                  font=("Courier", 10), wraplength=450, padx=10, pady=10)
        self.data_label.pack(fill="x", padx=10, pady=5)

        # Informacje
        info_text = tk.Label(self, text="1. Wyślij plik → 2. Wykonaj aktualizację (restart sterownika!)\n\n"
                                             "Ścieżka docelowa: /opt/plcnext/",
                                  fg="black", font=("Arial", 8))
        info_text.pack(pady=5)

    def select_file(self):
        """Otwiera okno dialogowe do wyboru pliku firmware."""
        filepath = filedialog.askopenfilename(title="Wybierz plik firmware")
        self.firmware_path.set(filepath)

    def log_status(self, msg, color="blue"):
        """Thread-safe aktualizacja statusu na etykiecie GUI."""
        self.after(0, lambda: self.status_label.config(text=msg, fg=color))

    def update_progress(self, percent):
        """Thread-safe aktualizacja paska postępu."""
        self.after(0, lambda: self.progress_var.set(percent))

    def sftp_progress(self, transferred, total):
        """Callback dla postępu SFTP - oblicza i wyświetla prędkość."""
        # Obliczenie procentu i aktualizacja paska postępu
        percent = (transferred / total) * 100
        self.update_progress(percent)
        
        current_time = time.time()
        
        # Sprawdzenie, czy minęła wystarczająca ilość czasu od ostatniej aktualizacji
        # Zapewnia stabilniejszy pomiar prędkości
        if current_time - self.last_time > 0.5:
            delta_transferred = transferred - self.last_transferred
            delta_time = current_time - self.last_time
            
            # Prędkość w bajtach na sekundę
            speed_bps = delta_transferred / delta_time
            
            # Konwersja na KB/s lub MB/s
            if speed_bps >= 1024 * 1024:
                speed_text = f"Prędkość: {speed_bps / (1024 * 1024):.2f} MB/s"
            else:
                speed_text = f"Prędkość: {speed_bps / 1024:.2f} KB/s"
            
            self.speed_var.set(speed_text)
            
            # Aktualizacja zmiennych do następnego pomiaru
            self.last_transferred = transferred
            self.last_time = current_time

        # Aktualizacja etykiety informacyjnej
        self.info_label.config(text=f"Postęp: {percent:.1f}%")

    def check_timezone_plc(self, ip_address, admin_password):
        """Sprawdza aktualną strefę czasową na PLC."""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip_address, username=PLC_USER, password=admin_password)
            stdin, stdout, stderr = ssh.exec_command("date && cat /etc/timezone")
            output = stdout.read().decode(errors="ignore").strip().split("\n")
            ssh.close()
            current_date = output[0].strip() if output else ""
            current_tz = output[1].strip() if len(output) > 1 else ""
            return current_date, current_tz
        except Exception as e:
            return None, f"Błąd: {e}"

    def change_timezone_plc(self, ip_address, admin_password):
        """Zmienia strefę czasową na PLC."""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip_address, username=PLC_USER, password=admin_password)
            shell = ssh.invoke_shell()

            def send_command(cmd, wait=1):
                shell.send(cmd + "\n")
                time.sleep(wait)

            send_command("sudo passwd root")
            send_command(admin_password)
            send_command(ROOT_PASS)
            send_command(ROOT_PASS)
            send_command("su")
            send_command(ROOT_PASS)
            send_command(f"ln -sf /usr/share/zoneinfo/{TIMEZONE} /etc/localtime")
            send_command(f"echo '{TIMEZONE}' > /etc/timezone")
            send_command("date")
            send_command("cat /etc/timezone")
            send_command("passwd -dl root")
            send_command("exit")
            send_command("sudo reboot")
            send_command(admin_password)
            time.sleep(2)
            output = shell.recv(65535).decode(errors="ignore")
            ssh.close()

            date_line = ""
            tz_line = ""
            for line in output.splitlines():
                if line.strip().startswith(("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")):
                    date_line = line.strip()
                elif "Europe/" in line:
                    tz_line = line.strip()
            return True, date_line, tz_line
        except Exception as e:
            return False, str(e), None

    def execute_firmware_update(self):
        """Wykonuje komendę aktualizacji firmware na sterowniku."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        plc_type = self.plc_type_var.get()
        if not ip or not password:
            messagebox.showerror("Błąd", "Podaj IP i hasło!")
            return
        response = messagebox.askyesno("Potwierdzenie",
                                       "Czy na pewno chcesz wykonać aktualizację firmware?\n"
                                       "Sterownik zostanie zrestartowany!\n\n"
                                       f"Komenda: sudo update-axcf{plc_type}")
        if not response:
            return
        threading.Thread(target=self.update_worker, args=(ip, password, plc_type), daemon=True).start()

    def update_worker(self, ip, password, plc_type):
        try:
            self.log_status("Łączenie z PLC dla aktualizacji...")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username="admin", password=password, timeout=30)
            self.log_status("Wykonywanie aktualizacji firmware...")
            update_cmd = f"sudo update-axcf{plc_type}"
            stdin, stdout, stderr = ssh.exec_command(update_cmd, get_pty=True)
            stdin.write(password + "\n")
            stdin.flush()
            output = stdout.read().decode(errors="ignore")
            errors = stderr.read().decode(errors="ignore")
            ssh.close()
            if "error" in output.lower() or "failed" in output.lower() or errors.strip():
                self.log_status("Błąd podczas aktualizacji", "red")
                self.after(0, lambda: messagebox.showerror("Błąd aktualizacji",
                                                          f"Błąd podczas aktualizacji:\n{output}\n{errors}"))
            else:
                self.log_status("Aktualizacja zakończona - sterownik restartuje się", "green")
                self.after(0, lambda: messagebox.showinfo("Sukces",
                                                          f"Aktualizacja firmware zakończona!\n\n"
                                                          f"Sterownik został zrestartowany.\n"
                                                          f"Wynik:\n{output[:200]}..."))
        except Exception as e:
            self.log_status("Błąd aktualizacji", "red")
            self.after(0, lambda: messagebox.showerror("Błąd", f"Błąd podczas aktualizacji: {str(e)}"))

    def test_connection(self):
        """Funkcja testowa do sprawdzenia połączenia."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        if not ip or not password:
            messagebox.showerror("Błąd", "Podaj IP i hasło!")
            return
        threading.Thread(target=self.test_worker, args=(ip, password), daemon=True).start()

    def test_worker(self, ip, password):
        try:
            self.log_status("Testowanie połączenia...")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username="admin", password=password, timeout=10)
            stdin, stdout, stderr = ssh.exec_command("pwd")
            result = stdout.read().decode().strip()
            ssh.close()
            self.log_status("Połączenie OK", "green")
            self.after(0, lambda: messagebox.showinfo("Test połączenia",
                                                      f"Połączenie udane!\nKatalog: {result}"))
        except Exception as e:
            self.log_status("Test połączenia - błąd", "red")
            self.after(0, lambda: messagebox.showerror("Test połączenia",
                                                      f"Błąd połączenia: {str(e)}"))

    def set_timezone(self):
        """Ustawia strefę czasową."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        if not ip or not password:
            messagebox.showerror("Błąd", "Podaj IP i hasło!")
            return
        threading.Thread(target=self.timezone_worker, args=(ip, password), daemon=True).start()

    def timezone_worker(self, ip, password):
        try:
            self.log_status("Sprawdzanie aktualnej strefy czasowej...")
            current_date, current_tz = self.check_timezone_plc(ip, password)
            if current_date is None:
                self.log_status("Błąd połączenia", "red")
                self.after(0, lambda: messagebox.showerror("Błąd", current_tz))
                return
            if current_tz == TIMEZONE:
                try:
                    plc_time_str = current_date.replace("CEST", "+0200").replace("CET", "+0100")
                    plc_time = datetime.strptime(plc_time_str, "%a %b %d %H:%M:%S %z %Y")
                    local_time = datetime.now(pytz.timezone(TIMEZONE))
                    diff_seconds = abs((local_time - plc_time).total_seconds())
                except Exception:
                    diff_seconds = None
                msg = (
                    f"Timezone jest już ustawiona na {TIMEZONE}.\n\n"
                    f"Aktualna data: {current_date}"
                )
                if diff_seconds is not None and diff_seconds > 60:
                    msg += (
                        "\n\n⚠ UWAGA: Czas sterownika różni się od czasu lokalnego "
                        "\nSprawdź w ustawieniach web PLC, czy włączony jest serwer NTP."
                        "\nSprawdź czy modem jest dobrze ustawiony."
                    )
                self.log_status("Strefa czasowa już ustawiona", "green")
                self.after(0, lambda: messagebox.showinfo("Informacja", msg))
                return
            self.log_status("Zmienianie strefy czasowej...")
            success, date_line, tz_line = self.change_timezone_plc(ip, password)
            if success:
                try:
                    plc_time_str = date_line.replace("CEST", "+0200").replace("CET", "+0100")
                    plc_time = datetime.strptime(plc_time_str, "%a %b %d %H:%M:%S %z %Y")
                    local_time = datetime.now(pytz.timezone(TIMEZONE))
                    diff_seconds = abs((local_time - plc_time).total_seconds())
                except Exception:
                    diff_seconds = None
                msg = (
                    f"Strefa czasowa została zmieniona.\n"
                    f"Z: {current_tz}\nNa: {tz_line}\n\n"
                    f"Aktualna data: {date_line}\n"
                    f"Sterownik został zrestartowany."
                )
                if diff_seconds is not None and diff_seconds > 60:
                    msg += (
                        "\n\n⚠ UWAGA: Czas sterownika różni się od czasu lokalnego "
                        "\nSprawdź w ustawieniach web PLC, czy włączony jest serwer NTP."
                        "\nSprawdź czy modem jest dobrze ustawiony."
                    )
                self.log_status("Strefa czasowa zmieniona", "green")
                self.after(0, lambda: messagebox.showinfo("Sukces", msg))
            else:
                self.log_status("Błąd zmiany strefy czasowej", "red")
                self.after(0, lambda: messagebox.showerror("Błąd", f"Operacja nieudana:\n{date_line}"))
        except Exception as e:
            self.log_status("Błąd operacji", "red")
            self.after(0, lambda: messagebox.showerror("Błąd", f"Błąd: {str(e)}"))

    def upload_firmware(self):
        """Przesyła plik firmware do sterownika przez SFTP."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        file_path = self.firmware_path.get()
        if not ip or not password or not file_path:
            messagebox.showerror("Błąd", "Uzupełnij wszystkie pola!")
            return
        filename = os.path.basename(file_path)
        remote_path = f"/opt/plcnext/{filename}"
        threading.Thread(target=self.upload_worker, args=(ip, password, file_path, remote_path), daemon=True).start()
    

    def read_plc_data(self):
        """Odczytuje dane (czas i wersję firmware) z PLC i wyświetla w oknie głównym."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        if not ip or not password:
            messagebox.showerror("Błąd", "Podaj IP i hasło!")
            return
        threading.Thread(target=self.read_worker, args=(ip, password), daemon=True).start()


    def upload_system_services(self):
        """Przesyła plik Default.scm.config do PLC."""
        ip = self.ip_entry.get()
        password = self.password_entry.get()
        if not ip or not password:
            messagebox.showerror("Błąd", "Podaj IP i hasło!")
            return
        
        # plik źródłowy – zawsze pakowany z aplikacją
        local_file = resource_path("Default.scm.config")
        if not os.path.exists(local_file):
            messagebox.showerror("Błąd", f"Plik nie istnieje:\n{local_file}")
            return

        remote_path = "/opt/plcnext/config/System/Scm/Default.scm.config"

        threading.Thread(
            target=self.upload_system_services_worker,
            args=(ip, password, local_file, remote_path),
            daemon=True
        ).start()

    def upload_system_services_worker(self, ip, password, local_file, remote_path):
        ssh = None
        sftp = None
        try:
            self.log_status("Łączenie z PLC (System Services)...")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username="admin", password=password, timeout=30)

            self.log_status("Wysyłanie pliku System Services...")
            sftp = ssh.open_sftp()
            sftp.put(local_file, remote_path)

            self.log_status("Plik System Services przesłany!", "green")
            self.after(0, lambda: messagebox.showinfo(
                "Sukces",
                f"Plik został przesłany do:\n{remote_path}"
            ))

        except Exception as e:
            self.log_status("Błąd transferu System Services", "red")
            self.after(0, lambda: messagebox.showerror(
                "Błąd",
                f"Błąd podczas przesyłania pliku System Services:\n{str(e)}"
            ))
        finally:
            try:
                if sftp:
                    sftp.close()
                if ssh:
                    ssh.close()
                self.log_status("Połączenie zamknięte")
            except:
                pass

    def upload_worker(self, ip, password, file_path, remote_path):
        ssh = None
        sftp = None
        # Zresetowanie zmiennych prędkości
        self.last_transferred = 0
        self.last_time = time.time()
        self.speed_var.set("Prędkość: 0.00 KB/s")
        
        try:
            self.log_status("Łączenie z PLC...")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username="admin", password=password, timeout=30)
            self.log_status("Wysyłanie firmware...")
            sftp = ssh.open_sftp()
            file_size = os.path.getsize(file_path)
            self.update_progress(0)
            sftp.put(file_path, remote_path, callback=self.sftp_progress)
            self.log_status("Weryfikowanie transferu...")
            
            try:
                remote_file_stat = sftp.stat(remote_path)
                remote_size = remote_file_stat.st_size
                if remote_size == file_size:
                    self.log_status("Transfer zakończony pomyślnie!", "green")
                    self.after(0, lambda: messagebox.showinfo("Sukces",
                                                            f"Plik został przesłany pomyślnie!\n"
                                                            f"Ścieżka: {remote_path}\n"
                                                            f"Rozmiar: {remote_size} bajtów\n\n"
                                                            f"Użyj przycisku 'Wykonaj aktualizację' aby zainstalować firmware."))
                else:
                    self.log_status("Błąd: Niepełny transfer!", "red")
                    self.after(0, lambda: messagebox.showerror("Błąd",
                                                            f"Transfer niepełny!\n"
                                                            f"Oczekiwano: {file_size} bajtów\n"
                                                            f"Otrzymano: {remote_size} bajtów"))
            except Exception as verify_error:
                self.log_status("Błąd weryfikacji transferu", "red")
                self.after(0, lambda: messagebox.showerror("Błąd weryfikacji",
                                                        f"Nie można zweryfikować transferu: {str(verify_error)}"))
        except paramiko.AuthenticationException:
            self.log_status("Błąd uwierzytelniania", "red")
            self.after(0, lambda: messagebox.showerror("Błąd", "Nieprawidłowe hasło!"))
        except paramiko.SSHException as ssh_error:
            self.log_status("Błąd SSH", "red")
            self.after(0, lambda: messagebox.showerror("Błąd SSH", str(ssh_error)))
        except FileNotFoundError:
            self.log_status("Plik nie znaleziony", "red")
            self.after(0, lambda: messagebox.showerror("Błąd", "Wybrany plik nie istnieje!"))
        except Exception as e:
            self.log_status("Błąd transferu", "red")
            self.after(0, lambda: messagebox.showerror("Błąd", f"Błąd podczas transferu: {str(e)}"))
        finally:
            try:
                if sftp:
                    sftp.close()
                if ssh:
                    ssh.close()
                    self.log_status("Połączenie zamknięte")
            except:
                pass
            if self.progress_var.get() < 100:
                self.update_progress(0)
            self.speed_var.set("Prędkość: -- KB/s")


    def upload_system_services_worker(self, ip, password, local_file, remote_path):
        ssh = None
        sftp = None
        try:
            self.log_status("Łączenie z PLC (System Services)...")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username="admin", password=password, timeout=30)

            self.log_status("Wysyłanie pliku System Services...")
            sftp = ssh.open_sftp()
            sftp.put(local_file, remote_path)

            self.log_status("System Services przesłany - restartowanie sterownika...", "green")
            
            # Automatyczny restart sterownika po przesłaniu System Services
            sftp.close()  # Zamykamy SFTP przed restartem
            sftp = None
            
            # Wykonujemy restart
            stdin, stdout, stderr = ssh.exec_command("sudo reboot", get_pty=True)
            stdin.write(password + "\n")
            stdin.flush()
            time.sleep(2)  # Chwila na wykonanie komendy
            
            self.after(0, lambda: messagebox.showinfo(
                "Sukces",
                f"Plik System Services został przesłany do:\n{remote_path}\n\n"
                f"Sterownik został automatycznie zrestartowany."
            ))
            self.log_status("System Services i restart zakończone!", "green")

        except Exception as e:
            self.log_status("Błąd transferu System Services", "red")
            self.after(0, lambda: messagebox.showerror(
                "Błąd",
                f"Błąd podczas przesyłania pliku System Services:\n{str(e)}"
            ))
        finally:
            try:
                if sftp:
                    sftp.close()
                if ssh:
                    ssh.close()
                self.log_status("Połączenie zamknięte")
            except:
                pass



    def read_worker(self, ip, password):
        try:
            self.log_status("Odczytywanie danych z PLC...")
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username="admin", password=password, timeout=10)

            # Odczyt czasu i strefy czasowej
            stdin_time, stdout_time, stderr_time = ssh.exec_command("date && cat /etc/timezone")
            time_output = stdout_time.read().decode(errors="ignore").strip().split("\n")
            current_date = time_output[0].strip() if time_output else "Brak danych o dacie"
            current_tz = time_output[1].strip() if len(time_output) > 1 else "Brak danych o strefie czasowej"

            # Odczyt wersji firmware
            stdin_ver, stdout_ver, stderr_ver = ssh.exec_command("grep Arpversion /etc/plcnext/arpversion")
            version_result = stdout_ver.read().decode().strip()
            if not version_result:
                version_result = "Brak informacji o wersji firmware."

            ssh.close()
            self.log_status("Dane z PLC odczytane.", "green")

            # Tworzenie tekstu do wyświetlenia
            display_text = (
                f"Aktualny czas: {current_date}\n"
                f"Strefa czasowa: {current_tz}\n\n"
                f"Wersja Firmware:\n{version_result}"
            )
            self.after(0, lambda: self.data_label.config(text=display_text))

        except paramiko.AuthenticationException:
            self.log_status("Błąd uwierzytelniania", "red")
            self.after(0, lambda: messagebox.showerror("Błąd", "Nieprawidłowe hasło!"))
        except Exception as e:
            self.log_status("Błąd odczytu danych", "red")
            self.after(0, lambda: messagebox.showerror("Błąd", f"Błąd podczas odczytu danych: {str(e)}"))
        finally:
            self.log_status("Gotowy", "blue")







if __name__ == "__main__":
    app = FirmwareUpdaterApp()
    app.mainloop()

