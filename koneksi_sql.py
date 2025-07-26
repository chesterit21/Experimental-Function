import pyodbc
import pandas as pd

# --- Konfigurasi Koneksi SQL Server Anda ---
# Ganti nilai-nilai di bawah ini dengan informasi SQL Server Anda
SERVER_NAME = 'MS-DL7390-DSFB7\SQLEXPRESS'  # Contoh: 'DESKTOP-ABCDE\SQLEXPRESS' atau 'localhost'
DATABASE_NAME = 'GamesMatrix' # Contoh: 'DataPenjualan'
TABLE_NAME = 'LogGame' # Contoh: 'Customers' atau 'Products'

# String koneksi menggunakan Windows Authentication
# Untuk SQL Server 2016, 'ODBC Driver 13 for SQL Server' juga umum,
# tapi 'ODBC Driver 17' adalah yang terbaru dan kompatibel.
# Pastikan driver yang Anda miliki sudah terinstal dan nama drivernya benar.
CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};" # ATAU 'ODBC Driver 13 for SQL Server'
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection=yes;" # Alternatif yang sering lebih andal untuk Windows Auth
    # f"ENCRYPT=no;" # Baris ini mungkin bisa dihapus jika tidak ada enkripsi
    # f"TRUSTSERVERCERTIFICATE=yes;" # Baris ini juga bisa dihapus untuk simplifikasi
)

# --- Sisanya kode tetap sama ---
try:
    print("Mencoba membuat koneksi ke SQL Server...")
    cnxn = pyodbc.connect(CONNECTION_STRING)
    print("Koneksi berhasil!")

    cursor = cnxn.cursor()
    print("Cursor berhasil dibuat.")

    query = f"SELECT top 10 * FROM {TABLE_NAME}"
    print(f"Menjalankan query: {query}")
    
    # Mengambil hasil query ke dalam DataFrame Pandas
    df = pd.read_sql(query, cnxn) # Cara yang lebih ringkas dengan Pandas

    print(f"\nData dari tabel '{TABLE_NAME}' berhasil diambil:")
    print(df.head())
    print(f"\nJumlah baris: {len(df)}")

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    print(f"Terjadi kesalahan koneksi atau query SQL:")
    print(f"SQLSTATE: {sqlstate}")
    print(f"Pesan Error: {ex.args[1]}")
    print("\nPastikan:")
    print("1. SERVER_NAME, DATABASE_NAME, dan TABLE_NAME sudah benar.")
    print("2. SQL Server Anda berjalan dan bisa diakses.")
    print("3. Akun Windows Anda memiliki izin akses ke database tersebut di SQL Server.")
    print("4. ODBC Driver for SQL Server (misal 'ODBC Driver 17 for SQL Server') sudah terinstal.")
    print("5. Mode Autentikasi SQL Server diatur ke 'SQL Server and Windows Authentication mode' dan service sudah di-restart.")

finally:
    if 'cnxn' in locals() and cnxn:
        cnxn.close()
        print("\nKoneksi SQL Server ditutup.")