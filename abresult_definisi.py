import pyodbc
import pandas as pd
import numpy as np
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
NUM_RESULTS_TO_OUTPUT = 9140 # <-- Anda bisa mengubah nilai ini!

# --- Fungsi untuk Mengambil Data ---
def get_log_game_data(server, database, table, conn_str):
    try:
        print(f"Mencoba mengambil data dari tabel '{table}'...")
        cnxn = pyodbc.connect(conn_str)
        query = f"SELECT Periode, LogResult, [As], Kop, Kepala, Ekor FROM {table} WHERE GameCode='TXM' ORDER BY Periode ASC"
        df = pd.read_sql(query, cnxn)
        print("Data berhasil diambil!")
        return df
    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Terjadi kesalahan saat mengambil data: SQLSTATE: {sqlstate} - Pesan: {ex.args[1]}")
        return None
    finally:
        if 'cnxn' in locals() and cnxn:
            cnxn.close()

### MODIFIKASI: Semua fungsi analisis sekarang bekerja dengan dictionary untuk melacak sumber ###

def add_near_log_candidates(predictions_dict, df, last_log_result_str, search_type, num_digits, window_size=1):
    """Menambahkan kandidat dari NearLog ke dictionary prediksi beserta sumbernya."""
    df_temp = df.copy()
    df_temp['LogResult_Str_Full'] = df_temp['LogResult'].astype(str).str.zfill(4)
    
    search_pattern = ""
    source_prefix = f"dari analisa {num_digits} Digit"
    
    if search_type == 'depan':
        search_pattern = last_log_result_str[:num_digits]
        source_prefix += f" Depan {search_pattern}"
        matching_rows = df_temp[df_temp['LogResult_Str_Full'].str.startswith(search_pattern)]
    elif search_type == 'tengah':
        search_pattern = last_log_result_str[1:num_digits+1]
        source_prefix += f" Tengah {search_pattern}"
        matching_rows = df_temp[df_temp['LogResult_Str_Full'].str[1:num_digits+1] == search_pattern]
    elif search_type == 'belakang':
        search_pattern = last_log_result_str[4-num_digits:]
        source_prefix += f" Belakang {search_pattern}"
        matching_rows = df_temp[df_temp['LogResult_Str_Full'].str.endswith(search_pattern)]
    else:
        return

    df_sorted = df_temp.sort_values(by='Periode', ascending=True).reset_index(drop=True)

    for idx_original_df in matching_rows.index:
        periode_match = df_temp.loc[idx_original_df, 'Periode']
        idx_in_sorted_df_match = df_sorted[df_sorted['Periode'] == periode_match]
        if idx_in_sorted_df_match.empty: continue
        idx_in_sorted_df = idx_in_sorted_df_match.index[0]
        
        start_idx = max(0, idx_in_sorted_df - window_size)
        end_idx = min(len(df_sorted) - 1, idx_in_sorted_df + window_size)
        
        for i in range(start_idx, end_idx + 1):
            log_result = df_sorted.loc[i, 'LogResult_Str_Full']
            if log_result not in predictions_dict:
                predictions_dict[log_result] = source_prefix

def add_analytical_candidates(predictions_dict, analysis_func, *args):
    """Helper untuk menambahkan kandidat dari fungsi analisis lain."""
    new_candidates = analysis_func(*args)
    for number, source in new_candidates.items():
        if number not in predictions_dict:
            predictions_dict[number] = source

def analyze_increase_decrease(df, last_log_result_str):
    new_candidates = {}
    df_temp = df.copy()
    df_temp['LogResult_Numeric'] = pd.to_numeric(df_temp['LogResult'].astype(str).str.zfill(4), errors='coerce').dropna()
    df_temp.sort_values(by='Periode', ascending=True, inplace=True)
    last_num_val = int(last_log_result_str)
    if len(df_temp) > 1:
        df_temp['Diff'] = df_temp['LogResult_Numeric'].diff()
        for diff in df_temp['Diff'].dropna().unique():
            candidate = str(int(last_num_val + diff)).zfill(4)
            if len(candidate) == 4 and candidate.isdigit():
                new_candidates[candidate] = f"dari Analisis Kenaikan/Penurunan (selisih {int(diff)})"
    return new_candidates

def analyze_jump_values(df, last_log_result_str):
    new_candidates = {}
    df_temp = df.copy()
    df_temp['LogResult_Numeric'] = pd.to_numeric(df_temp['LogResult'].astype(str).str.zfill(4), errors='coerce').dropna()
    df_temp.sort_values(by='Periode', ascending=True, inplace=True)
    last_num_val = int(last_log_result_str)
    if not df_temp.empty:
        jump_diffs = []
        for i in range(len(df_temp)):
            for j in range(1, 4): # Jumps 1, 2, 3 periode
                if i - j >= 0:
                    diff = df_temp.loc[i, 'LogResult_Numeric'] - df_temp.loc[i - j, 'LogResult_Numeric']
                    if diff != 0: jump_diffs.append(diff)
        if jump_diffs:
            for jump, _ in Counter(jump_diffs).most_common(10):
                candidate = str(int(last_num_val + jump)).zfill(4)
                if len(candidate) == 4 and candidate.isdigit():
                    new_candidates[candidate] = f"dari Analisis Lompatan Nilai (lompatan {int(jump)})"
    return new_candidates

def expand_candidates_iteratively(base_predictions_dict, target_count):
    """
    Mengembangbiakkan kandidat hingga mencapai jumlah target dengan memodifikasi digit
    secara iteratif dan mencatat sumber ekspansinya.
    """
    final_dict = base_predictions_dict.copy()
    source_candidates = list(base_predictions_dict.keys())
    
    level = 1
    while len(final_dict) < target_count:
        print(f"  Jumlah kandidat ({len(final_dict)}) belum cukup. Memulai ekspansi level {level}...")
        
        newly_generated_in_level = {}
        mod_combinations = list(set((i, j) for i in range(1, level + 1) for j in range(1, level + 1)))

        for number_str in source_candidates:
            if len(final_dict) >= target_count: break
            num_list = [int(d) for d in number_str]
            for digit_idx in range(4):
                original_digit = num_list[digit_idx]
                for mod_up, mod_down in mod_combinations:
                    # Coba Naik
                    new_digit_up = original_digit + mod_up
                    if 0 <= new_digit_up <= 9:
                        temp_list = list(num_list)
                        temp_list[digit_idx] = new_digit_up
                        new_num = "".join(map(str, temp_list))
                        if new_num not in final_dict and new_num not in newly_generated_in_level:
                            newly_generated_in_level[new_num] = f"ekspansi level {level} dari {number_str}"
                    
                    # Coba Turun
                    new_digit_down = original_digit - mod_down
                    if 0 <= new_digit_down <= 9:
                        temp_list = list(num_list)
                        temp_list[digit_idx] = new_digit_down
                        new_num = "".join(map(str, temp_list))
                        if new_num not in final_dict and new_num not in newly_generated_in_level:
                             newly_generated_in_level[new_num] = f"ekspansi level {level} dari {number_str}"
        
        if not newly_generated_in_level:
            print("  Ekspansi level ini tidak menghasilkan kandidat baru. Menghentikan proses.")
            break
        
        final_dict.update(newly_generated_in_level)
        level += 1

    return final_dict

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

        # Dictionary untuk menyimpan nomor unik dan sumbernya
        all_predicted_numbers_dict = {}

        # --- Tahap 1: Pengumpulan Kandidat Awal ---
        print("\n--- Tahap 1: Mengumpulkan Kandidat Awal dari Analisis Historis ---")
        
        # Analisis NearLog 2 Digit
        add_near_log_candidates(all_predicted_numbers_dict, df_search_historical, last_log_result_str, 'depan', 2)
        add_near_log_candidates(all_predicted_numbers_dict, df_search_historical, last_log_result_str, 'tengah', 2)
        add_near_log_candidates(all_predicted_numbers_dict, df_search_historical, last_log_result_str, 'belakang', 2)
        print(f"Kandidat setelah Analisis NearLog 2-Digit: {len(all_predicted_numbers_dict)}")

        # Analisis NearLog 3 Digit
        add_near_log_candidates(all_predicted_numbers_dict, df_search_historical, last_log_result_str, 'depan', 3)
        add_near_log_candidates(all_predicted_numbers_dict, df_search_historical, last_log_result_str, 'tengah', 3)
        add_near_log_candidates(all_predicted_numbers_dict, df_search_historical, last_log_result_str, 'belakang', 3)
        print(f"Kandidat setelah Analisis NearLog 3-Digit: {len(all_predicted_numbers_dict)}")

        # Analisis Pola Lompatan
        add_analytical_candidates(all_predicted_numbers_dict, analyze_increase_decrease, df_search_historical, last_log_result_str)
        print(f"Kandidat setelah Analisis Kenaikan/Penurunan: {len(all_predicted_numbers_dict)}")
        
        add_analytical_candidates(all_predicted_numbers_dict, analyze_jump_values, df_search_historical, last_log_result_str)
        print(f"Kandidat setelah Analisis Lompatan Nilai: {len(all_predicted_numbers_dict)}")

        # --- Tahap 2: Ekspansi Kandidat ---
        print(f"\n--- Tahap 2: Memeriksa dan Mengekspansi Kandidat ---")
        
        final_predictions = all_predicted_numbers_dict
        
        if len(final_predictions) < NUM_RESULTS_TO_OUTPUT:
            print(f"Jumlah kandidat awal ({len(final_predictions)}) kurang dari target ({NUM_RESULTS_TO_OUTPUT}).")
            final_predictions = expand_candidates_iteratively(
                base_predictions_dict=final_predictions,
                target_count=NUM_RESULTS_TO_OUTPUT
            )
        
        # Ambil list dari dictionary untuk pemotongan dan penulisan
        final_output_list = list(final_predictions.items())[:NUM_RESULTS_TO_OUTPUT]

        # --- Tahap 3: Menampilkan Hasil Akhir ---
        print(f"\n--- Hasil Akhir ---")
        print(f"Total nomor unik yang dihasilkan: {len(final_output_list)}")
        
        if final_output_list:
            print(f"\nContoh {min(len(final_output_list), 10)} Nomor yang Dihasilkan Beserta Sumbernya:")
            for i, (number, source) in enumerate(final_output_list[:10]):
                print(f"{i+1}. {number} --> {source}")
            
            output_filename = f'predicted_numbers_with_source_MQ23_V1_{len(final_output_list)}.txt'
            with open(output_filename, 'w') as f:
                for number, source in final_output_list:
                    f.write(f"{number} --> {source}\n")
            
            print(f"\nSemua {len(final_output_list)} nomor telah disimpan ke '{output_filename}'")
        else:
            print("\nTidak ada nomor yang berhasil diprediksi/dihasilkan.")

        print("Pemodelan dan prediksi selesai!")

    else:
        print("Gagal mendapatkan data atau data kosong, tidak bisa melanjutkan proses.")