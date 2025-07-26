import pyodbc
import pandas as pd
import numpy as np
import itertools
from collections import Counter

# --- Konfigurasi Koneksi SQL Server Anda ---
SERVER_NAME = 'MS-DL7390-DSFB7\SQLEXPRESS'
DATABASE_NAME = 'GamesMatrix'
TABLE_NAME = 'LogGame'

CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER_NAME};"
    f"DATABASE={DATABASE_NAME};"
    f"Trusted_Connection=yes;"
)

# --- Variabel Global untuk Jumlah Hasil Prediksi ---
NUM_RESULTS_TO_OUTPUT = 8800 # <-- Anda bisa mengubah nilai ini!

# --- Fungsi untuk Mengambil Data ---
def get_log_game_data(server, database, table, conn_str):
    try:
        print(f"Mencoba mengambil data dari tabel '{table}'...")
        cnxn = pyodbc.connect(conn_str)
        query = f"SELECT Periode, LogResult, [As], Kop, Kepala, Ekor FROM {table} WHERE GameCode='PS' ORDER BY Periode ASC"
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

# --- Fungsi untuk Mendapatkan NearLog ---
def get_near_logs(df, current_log_result_str, search_type, num_digits, window_size=1):
    df_temp = df.copy()
    df_temp['LogResult_Str_Full'] = df_temp['LogResult'].astype(str).str.zfill(4)
    
    search_pattern = ""
    if search_type == 'depan':
        search_pattern = current_log_result_str[:num_digits]
        matching_rows = df_temp[df_temp['LogResult_Str_Full'].str.startswith(search_pattern)]
    elif search_type == 'tengah':
        if num_digits == 2:
            search_pattern = current_log_result_str[1:3]
            matching_rows = df_temp[df_temp['LogResult_Str_Full'].str[1:3] == search_pattern]
        elif num_digits == 3:
            search_pattern = current_log_result_str[1:4] 
            matching_rows = df_temp[df_temp['LogResult_Str_Full'].str[1:4] == search_pattern]
    elif search_type == 'belakang':
        search_pattern = current_log_result_str[4-num_digits:]
        matching_rows = df_temp[df_temp['LogResult_Str_Full'].str.endswith(search_pattern)]
    else:
        return []

    near_logs_list = []
    df_sorted = df_temp.sort_values(by='Periode', ascending=True).reset_index(drop=True)

    for idx_original_df in matching_rows.index:
        idx_in_sorted_df_match = df_sorted[df_sorted['Periode'] == df_temp.loc[idx_original_df, 'Periode']]
        if idx_in_sorted_df_match.empty:
            continue
        idx_in_sorted_df = idx_in_sorted_df_match.index[0]
        
        start_idx = max(0, idx_in_sorted_df - window_size)
        end_idx = min(len(df_sorted) - 1, idx_in_sorted_df + window_size)
        
        near_log_data = [{'Periode': df_sorted.loc[i, 'Periode'], 'LogResult': df_sorted.loc[i, 'LogResult_Str_Full']} for i in range(start_idx, end_idx + 1)]
        
        near_logs_list.append({
            'Periode': df_sorted.loc[idx_in_sorted_df, 'Periode'],
            'LogResult': df_sorted.loc[idx_in_sorted_df, 'LogResult_Str_Full'],
            'NearLog': near_log_data
        })
    
    return near_logs_list

# --- Fungsi Analisis (Lompatan, Tren, Digit, dll.) ---
def analyze_increase_decrease(df, last_log_result_str, window_size=5):
    df_temp = df.copy()
    df_temp['LogResult_Numeric'] = pd.to_numeric(df_temp['LogResult'].astype(str).str.zfill(4), errors='coerce').dropna()
    df_temp.sort_values(by='Periode', ascending=True, inplace=True)
    df_temp.reset_index(drop=True, inplace=True)
    candidates = []
    last_num_val = int(last_log_result_str)
    if len(df_temp) > 1:
        df_temp['Diff'] = df_temp['LogResult_Numeric'].diff()
        for diff in df_temp['Diff'].dropna().unique():
            candidates.append(str(int(last_num_val + diff)).zfill(4))
    valid_candidates = [c for c in candidates if len(c) == 4 and c.isdigit()]
    return list(set(valid_candidates))

def analyze_jump_values(df, last_log_result_str, num_jumps_to_consider=3):
    df_temp = df.copy()
    df_temp['LogResult_Numeric'] = pd.to_numeric(df_temp['LogResult'].astype(str).str.zfill(4), errors='coerce').dropna()
    df_temp.sort_values(by='Periode', ascending=True, inplace=True)
    df_temp.reset_index(drop=True, inplace=True)
    candidates = []
    last_num_val = int(last_log_result_str)
    if not df_temp.empty:
        jump_diffs = []
        for i in range(len(df_temp)):
            for j in range(1, num_jumps_to_consider + 1):
                if i - j >= 0:
                    diff = df_temp.loc[i, 'LogResult_Numeric'] - df_temp.loc[i - j, 'LogResult_Numeric']
                    if diff != 0: jump_diffs.append(diff)
        if jump_diffs:
            for jump, _ in Counter(jump_diffs).most_common(10):
                candidates.append(str(int(last_num_val + jump)).zfill(4))
    valid_candidates = [c for c in candidates if len(c) == 4 and c.isdigit()]
    return list(set(valid_candidates))

def analyze_most_frequent_digits(df):
    digit_cols = ['As', 'Kop', 'Kepala', 'Ekor']
    candidates = set()
    for col in digit_cols:
        df_col_cleaned = pd.to_numeric(df[col], errors='coerce').dropna().astype(int)
        if not df_col_cleaned.empty:
            for mode_digit in df_col_cleaned.mode():
                # Ini adalah pendekatan sederhana, bisa dikembangkan lebih lanjut
                # Untuk saat ini, kita tidak membuat kombinasi dari mode digit
                pass
    return list(candidates)


### BARU: Fungsi untuk ekspansi kandidat secara iteratif ###
def expand_candidates_iteratively(base_candidates, target_count):
    """
    Mengembangbiakkan kandidat hingga mencapai jumlah target dengan memodifikasi digit
    secara iteratif berdasarkan level.
    """
    if not base_candidates:
        print("  Tidak ada kandidat dasar untuk diekspansi.")
        return []

    final_set = set(base_candidates)
    
    # Gunakan list dari kandidat dasar asli untuk setiap level ekspansi
    # agar tidak terjadi ledakan kombinasi dari angka yang baru dibuat.
    source_candidates = list(base_candidates)
    
    level = 1
    while len(final_set) < target_count:
        print(f"  Jumlah kandidat ({len(final_set)}) belum cukup. Memulai ekspansi level {level}...")
        
        newly_generated_in_level = set()
        
        # Buat kombinasi modifikasi untuk level saat ini
        # Level 1: (+1,-1)
        # Level 2: (+1,-1), (+1,-2), (+2,-1), (+2,-2)
        # Level 3: dst...
        mod_combinations = []
        for i in range(1, level + 1):
            for j in range(1, level + 1):
                mod_combinations.append((i, j))
        
        mod_combinations = list(set(mod_combinations)) # Hapus duplikat

        for number_str in source_candidates:
            num_list = [int(d) for d in number_str]
            for digit_idx in range(4): # 0=As, 1=Kop, 2=Kepala, 3=Ekor
                original_digit = num_list[digit_idx]
                
                for mod_up, mod_down in mod_combinations:
                    # Coba Naik
                    new_digit_up = original_digit + mod_up
                    if 0 <= new_digit_up <= 9:
                        temp_list = list(num_list)
                        temp_list[digit_idx] = new_digit_up
                        newly_generated_in_level.add("".join(map(str, temp_list)))

                    # Coba Turun
                    new_digit_down = original_digit - mod_down
                    if 0 <= new_digit_down <= 9:
                        temp_list = list(num_list)
                        temp_list[digit_idx] = new_digit_down
                        newly_generated_in_level.add("".join(map(str, temp_list)))

        previous_count = len(final_set)
        final_set.update(newly_generated_in_level)
        
        if len(final_set) == previous_count:
            print("  Ekspansi level ini tidak menghasilkan kandidat baru yang unik. Menghentikan proses.")
            break
            
        level += 1

    return list(final_set)


# --- Main Program ---
if __name__ == "__main__":
    df_log = get_log_game_data(SERVER_NAME, DATABASE_NAME, TABLE_NAME, CONNECTION_STRING)

    if df_log is not None and not df_log.empty:
        df_log['LogResult_Str'] = df_log['LogResult'].astype(str).str.zfill(4)
        last_log_result_data = df_log.sort_values(by='Periode', ascending=False).iloc[0]
        last_log_result_str = last_log_result_data['LogResult_Str']
        last_periode = last_log_result_data['Periode']
        print(f"\nLogResult terakhir (Periode {last_periode}): {last_log_result_str}")
        
        df_search_historical = df_log[df_log['Periode'] < last_periode].copy()

        # Inisialisasi set untuk menyimpan semua nomor unik dari berbagai metode
        all_predicted_numbers = set()

        # --- Tahap 1: Pengumpulan Kandidat Awal ---
        print("\n--- Tahap 1: Mengumpulkan Kandidat Awal dari Analisis Historis ---")

        # Analisis NearLog
        near_logs_2d = get_near_logs(df_search_historical, last_log_result_str, 'depan', 2) + \
                       get_near_logs(df_search_historical, last_log_result_str, 'tengah', 2) + \
                       get_near_logs(df_search_historical, last_log_result_str, 'belakang', 2)
        if near_logs_2d:
            for entry in near_logs_2d:
                for log in entry['NearLog']:
                    all_predicted_numbers.add(log['LogResult'])

        near_logs_3d = get_near_logs(df_search_historical, last_log_result_str, 'depan', 3) + \
                       get_near_logs(df_search_historical, last_log_result_str, 'tengah', 3) + \
                       get_near_logs(df_search_historical, last_log_result_str, 'belakang', 3)
        if near_logs_3d:
            for entry in near_logs_3d:
                for log in entry['NearLog']:
                    all_predicted_numbers.add(log['LogResult'])
        print(f"Kandidat setelah Analisis NearLog: {len(all_predicted_numbers)}")

        # Analisis Pola Lompatan
        all_predicted_numbers.update(analyze_increase_decrease(df_search_historical, last_log_result_str))
        print(f"Kandidat setelah Analisis Kenaikan/Penurunan: {len(all_predicted_numbers)}")
        
        all_predicted_numbers.update(analyze_jump_values(df_search_historical, last_log_result_str))
        print(f"Kandidat setelah Analisis Lompatan Nilai: {len(all_predicted_numbers)}")

        # --- Tahap 2: Ekspansi Kandidat Secara Iteratif ---
        print(f"\n--- Tahap 2: Memeriksa dan Mengekspansi Kandidat ---")
        
        final_list_for_output = list(all_predicted_numbers)
        
        if len(final_list_for_output) < NUM_RESULTS_TO_OUTPUT:
            print(f"Jumlah kandidat awal ({len(final_list_for_output)}) kurang dari target ({NUM_RESULTS_TO_OUTPUT}).")
            final_list_for_output = expand_candidates_iteratively(
                base_candidates=final_list_for_output,
                target_count=NUM_RESULTS_TO_OUTPUT
            )
        
        # Pastikan jumlah akhir sesuai target dengan memotong jika berlebih
        final_list_for_output = final_list_for_output[:NUM_RESULTS_TO_OUTPUT]

        # --- Tahap 3: Menampilkan Hasil Akhir ---
        print(f"\n--- Hasil Akhir ---")
        print(f"Total nomor unik yang dihasilkan: {len(final_list_for_output)}")
        
        if final_list_for_output:
            print(f"\n{min(len(final_list_for_output), 10)} Nomor Teratas yang Dihasilkan:")
            for i, number in enumerate(final_list_for_output[:10]):
                print(f"{i+1}. Nomor: {number}")
            
            output_filename = f'predicted_numbers_expanded_MQ20_{len(final_list_for_output)}.txt'
            with open(output_filename, 'w') as f:
                for number in final_list_for_output:
                    f.write(f"{number}\n")
            
            print(f"\nSemua {len(final_list_for_output)} nomor telah disimpan ke '{output_filename}'")
        else:
            print("\nTidak ada nomor yang berhasil diprediksi/dihasilkan.")

        print("Pemodelan dan prediksi selesai!")

    else:
        print("Gagal mendapatkan data atau data kosong, tidak bisa melanjutkan proses.")