import pyodbc
import pandas as pd
import numpy as np # Import numpy juga

# --- Konfigurasi Koneksi SQL Server Anda ---
# Ganti nilai-nilai di bawah ini dengan informasi SQL Server Anda yang sudah berhasil kemarin
SERVER_NAME = 'MS-DL7390-DSFB7\SQLEXPRESS'  # Contoh: 'DESKTOP-ABCDE\SQLEXPRESS' atau 'localhost'
DATABASE_NAME = 'GamesMatrix' # Contoh: 'DataPenjualan'
TABLE_NAME = 'LogGame' # Contoh: 'Customers' atau 'Products'

CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection=yes;"
)

# --- Fungsi untuk Mengambil Data ---
def get_log_game_data(server, database, table, conn_str):
    try:
        print(f"Mencoba mengambil data dari tabel '{table}'...")
        cnxn = pyodbc.connect(conn_str)
        # Mengurutkan berdasarkan Periode sangat penting untuk time series
        query = f"SELECT Periode, LogResult, [As], Kop, Kepala, Ekor FROM {table} WHERE GameCode='MQ18' ORDER BY Periode ASC"
        df = pd.read_sql(query, cnxn)
        print("Data berhasil diambil!")
        return df
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Terjadi kesalahan saat mengambil data:")
        print(f"SQLSTATE: {sqlstate}")
        print(f"Pesan Error: {ex.args[1]}")
        return None
    finally:
        if 'cnxn' in locals() and cnxn:
            cnxn.close()

# --- Main Program ---
if __name__ == "__main__":
    df_log = get_log_game_data(SERVER_NAME, DATABASE_NAME, TABLE_NAME, CONNECTION_STRING)

    if df_log is not None:
        print("\n--- Pra-pemrosesan Data dan Pembuatan Fitur ---")

        # 1. Konversi LogResult ke string 4 digit dengan leading zeros, lalu ke numerik
        # Ini penting agar "0123" tidak jadi 123
        df_log['LogResult_Str'] = df_log['LogResult'].astype(str).str.zfill(4)
        df_log['LogResult_Numeric'] = pd.to_numeric(df_log['LogResult_Str'], errors='coerce')
        
        # Pastikan tidak ada NaN setelah konversi
        if df_log['LogResult_Numeric'].isnull().any():
            print("\n!!! Peringatan: Ada nilai non-numerik di LogResult yang menjadi NaN. Periksa data asli.")
            # Anda bisa memutuskan untuk menghapus baris NaN atau mengisinya
            df_log.dropna(subset=['LogResult_Numeric'], inplace=True)
            print("Baris dengan LogResult non-numerik telah dihapus.")

        # 2. Membuat Fitur Lag (nilai dari periode sebelumnya)
        # Kita akan membuat lag untuk setiap digit (A, B, C, D)
        # Jumlah lag bisa disesuaikan, kita coba 3 periode sebelumnya dulu
        num_lags = 3
        for col in ['As', 'Kop', 'Kepala', 'Ekor']:
            for i in range(1, num_lags + 1):
                df_log[f'{col}_lag{i}'] = df_log[col].shift(i)
        
        # Setelah membuat lag, baris pertama (num_lags) akan memiliki nilai NaN
        # Kita perlu menghapusnya karena tidak ada data historis untuk memprediksi mereka
        df_log.dropna(inplace=True)
        
        # Konversi kolom lag ke integer (karena hasil shift bisa menjadi float)
        for col in ['As', 'Kop', 'Kepala', 'Ekor']:
            for i in range(1, num_lags + 1):
                df_log[f'{col}_lag{i}'] = df_log[f'{col}_lag{i}'].astype(int)

        print(f"\nDataFrame setelah membuat {num_lags} fitur lag dan menghapus baris NaN:")
        print(df_log.head())
        print(f"Ukuran DataFrame sekarang: {df_log.shape}")
        
        # --- Anda bisa menambahkan langkah eksplorasi lagi di sini jika mau ---
        # Contoh: Korelasi antar kolom lag
        # print("\nKorelasi antar fitur lag:")
        # print(df_log[[f'{col}_lag{i}' for col in ['A', 'B', 'C', 'D'] for i in range(1, num_lags + 1)]].corr())

        print("\nData siap untuk pemodelan!")
        
        # Sekarang, df_log sudah memiliki fitur lag yang bisa digunakan sebagai input model
        # df_log akan berisi kolom-kolom seperti: Periode, LogResult, A, B, C, D, LogResult_Str, LogResult_Numeric, A_lag1, A_lag2, A_lag3, B_lag1, ... D_lag3

    else:
        print("Gagal mendapatkan data, tidak bisa melanjutkan pra-pemrosesan.")