import pyodbc
import pandas as pd
from collections import Counter
from tqdm import tqdm
import json
import os

# --- Konfigurasi Utama ---
GAME_CODE = 'TXM' # <-- UBAH NILAI INI UNTUK GAMECODE YANG BERBEDA
BENCHMARK_FILENAME = f"{GAME_CODE}_benchmark_patterns.json"

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

# --- Variabel Global ---
NUM_RESULTS_TO_OUTPUT = 9200
BACKTEST_TARGET_COUNT = 9000
NUM_BENCHMARK_PATTERNS = 15
INCREMENTAL_BACKTEST_PERIODS = 30 # Jumlah periode terakhir untuk backtest inkremental

# --- Fungsi Bantuan & Analisis ---
def get_log_game_data(server, database, table, conn_str, game_code):
    try:
        print(f"Mencoba mengambil data dari tabel '{table}' untuk GameCode '{game_code}'...")
        cnxn = pyodbc.connect(conn_str)
        query = f"SELECT Periode, LogResult FROM {table} WHERE GameCode='{game_code}' ORDER BY Periode ASC"
        df = pd.read_sql(query, cnxn)
        if df.empty:
            print(f"Peringatan: Tidak ada data yang ditemukan untuk GameCode '{game_code}'.")
            return None
        df['LogResult_Str'] = df['LogResult'].astype(str).str.zfill(4)
        print(f"Data berhasil diambil! Total {len(df)} periode.")
        return df
    except pyodbc.Error as ex:
        print(f"Terjadi kesalahan saat mengambil data: {ex.args[1]}")
        return None

def save_benchmark_patterns(filename, patterns):
    """Menyimpan daftar pola benchmark ke file JSON."""
    try:
        with open(filename, 'w') as f:
            json.dump(patterns, f, indent=4)
        print(f"Pola benchmark telah berhasil disimpan ke '{filename}'")
    except Exception as e:
        print(f"Gagal menyimpan file benchmark: {e}")

def load_benchmark_patterns(filename):
    """Memuat daftar pola benchmark dari file JSON."""
    if not os.path.exists(filename):
        return None
    try:
        with open(filename, 'r') as f:
            patterns = json.load(f)
            # Pastikan file tidak kosong dan isinya adalah list
            if isinstance(patterns, list) and patterns:
                return patterns
            else:
                return None # Anggap tidak valid jika kosong atau bukan list
    except (json.JSONDecodeError, Exception) as e:
        print(f"Gagal memuat atau file benchmark korup ('{filename}'): {e}")
        return None

# ... (Fungsi extract_pattern_type, add_near_log_candidates, expand_candidates_iteratively, generate_full_prediction_set tidak berubah)...
def extract_pattern_type(source_string):
    if not isinstance(source_string, str): return "Sumber Tidak Diketahui"
    parts = source_string.split()
    if source_string.startswith("ekspansi level"): return f"Ekspansi Level {parts[2]}"
    if source_string.startswith("dari analisa"): return f"Analisa {parts[2]} Digit {parts[3]}"
    if source_string.startswith("dari Analisis Kenaikan/Penurunan"): return "Analisis Kenaikan/Penurunan"
    if source_string.startswith("dari Analisis Lompatan Nilai"): return "Analisis Lompatan Nilai"
    return source_string

def add_near_log_candidates(predictions_dict, df, last_log, search_type, num_digits, window=1):
    source_prefix = f"dari analisa {num_digits} Digit"
    pattern = ""
    if search_type == 'depan':
        pattern = last_log[:num_digits]; matching_rows = df[df['LogResult_Str'].str.startswith(pattern)]; source_prefix += f" Depan {pattern}"
    elif search_type == 'tengah':
        pattern = last_log[1:1+num_digits]; matching_rows = df[df['LogResult_Str'].str[1:1+num_digits] == pattern]; source_prefix += f" Tengah {pattern}"
    elif search_type == 'belakang':
        pattern = last_log[4-num_digits:]; matching_rows = df[df['LogResult_Str'].str.endswith(pattern)]; source_prefix += f" Belakang {pattern}"
    else: return
    for _, row in matching_rows.iterrows():
        start_idx = max(0, row.name - window); end_idx = min(len(df) - 1, row.name + window)
        for i in range(start_idx, end_idx + 1):
            log_result = df.iloc[i]['LogResult_Str']
            if log_result not in predictions_dict: predictions_dict[log_result] = source_prefix

def expand_candidates_iteratively(base_dict, target_count):
    final_dict = base_dict.copy(); source_keys = list(base_dict.keys()); level = 1
    while len(final_dict) < target_count:
        newly_gen = {}; mod_combs = list(set((i, j) for i in range(1, level + 1) for j in range(1, level + 1)))
        for num_str in source_keys:
            if len(final_dict) + len(newly_gen) >= target_count: break
            num_list = [int(d) for d in num_str]
            for d_idx in range(4):
                for mod_up, mod_down in mod_combs:
                    new_d_up = num_list[d_idx] + mod_up
                    if 0 <= new_d_up <= 9:
                        new_l = list(num_list); new_l[d_idx] = new_d_up; new_n = "".join(map(str, new_l))
                        if new_n not in final_dict and new_n not in newly_gen: newly_gen[new_n] = f"ekspansi level {level} dari {num_str}"
                    new_d_down = num_list[d_idx] - mod_down
                    if 0 <= new_d_down <= 9:
                        new_l = list(num_list); new_l[d_idx] = new_d_down; new_n = "".join(map(str, new_l))
                        if new_n not in final_dict and new_n not in newly_gen: newly_gen[new_n] = f"ekspansi level {level} dari {num_str}"
        if not newly_gen: break
        final_dict.update(newly_gen); level += 1
    return final_dict

def generate_full_prediction_set(historical_df, last_log_result_str, target_count):
    predictions = {}
    for nd in [2, 3]:
        for st in ['depan', 'tengah', 'belakang']:
            add_near_log_candidates(predictions, historical_df, last_log_result_str, st, nd)
    if len(predictions) < target_count:
        predictions = expand_candidates_iteratively(predictions, target_count)
    return predictions


# --- Program Utama ---
if __name__ == "__main__":
    df_log = get_log_game_data(SERVER_NAME, DATABASE_NAME, TABLE_NAME, CONNECTION_STRING, GAME_CODE)

    if df_log is not None and len(df_log) > 1:
        
        # =================================================================
        # FASE 1: BACKTESTING & MANAJEMEN BENCHMARK
        # =================================================================
        print(f"\n--- FASE 1: Memeriksa dan Menjalankan Backtesting untuk '{GAME_CODE}' ---")
        
        # Coba muat benchmark yang ada
        existing_patterns = load_benchmark_patterns(BENCHMARK_FILENAME)
        pattern_benchmark_counter = Counter()
        backtest_start_index = 1

        if existing_patterns:
            print(f"File benchmark '{BENCHMARK_FILENAME}' ditemukan. Memuat {len(existing_patterns)} pola.")
            # Beri "bobot" awal pada pola yang ada agar tetap relevan
            pattern_benchmark_counter.update(existing_patterns)
            # Tentukan titik mulai untuk backtest inkremental
            backtest_start_index = max(1, len(df_log) - INCREMENTAL_BACKTEST_PERIODS)
            print(f"Akan menjalankan backtest inkremental untuk {INCREMENTAL_BACKTEST_PERIODS} periode terakhir (mulai dari indeks {backtest_start_index})...")
        else:
            print(f"File benchmark '{BENCHMARK_FILENAME}' tidak ditemukan atau kosong. Menjalankan backtest penuh...")
            # `backtest_start_index` sudah 1, jadi tidak perlu diubah.

        # Jalankan loop backtesting (baik penuh maupun inkremental)
        for i in tqdm(range(backtest_start_index, len(df_log) - 1), desc=f"Backtesting {GAME_CODE}"):
            previous_period_log = df_log.iloc[i-1]['LogResult_Str']
            current_period_actual_log = df_log.iloc[i]['LogResult_Str']
            historical_data_for_test = df_log.iloc[:i]
            
            predictions_for_test = generate_full_prediction_set(historical_data_for_test, previous_period_log, BACKTEST_TARGET_COUNT)
            
            if current_period_actual_log in predictions_for_test:
                winning_source = predictions_for_test[current_period_actual_log]
                pattern_type = extract_pattern_type(winning_source)
                pattern_benchmark_counter[pattern_type] += 1
        
        print("\n--- Hasil Backtesting & Pembaruan Benchmark ---")
        if not pattern_benchmark_counter:
            print("Tidak ada pola benchmark yang ditemukan.")
            benchmark_patterns = []
        else:
            # Dapatkan daftar pola teratas dari counter yang sudah diperbarui
            benchmark_patterns = [p for p, c in pattern_benchmark_counter.most_common(NUM_BENCHMARK_PATTERNS)]
            print(f"{len(benchmark_patterns)} Pola Benchmark Teratas (setelah pembaruan):")
            for i, pattern in enumerate(benchmark_patterns):
                print(f"{i+1}. '{pattern}' (Total Poin: {pattern_benchmark_counter[pattern]})")
            
            # Simpan hasil benchmark terbaru ke file JSON
            save_benchmark_patterns(BENCHMARK_FILENAME, benchmark_patterns)

        # =================================================================
        # FASE 2: PREDIKSI FINAL MENGGUNAKAN BENCHMARK
        # =================================================================
        print(f"\n--- FASE 2: Membuat Prediksi Final untuk '{GAME_CODE}'---")
        
        final_last_log = df_log.iloc[-1]['LogResult_Str']
        print(f"LogResult terakhir untuk prediksi: {final_last_log} (dari Periode {df_log.iloc[-1]['Periode']})")

        final_predictions_dict = generate_full_prediction_set(df_log, final_last_log, NUM_RESULTS_TO_OUTPUT)
        
        prioritized_results = []; other_results = []
        for number, source in final_predictions_dict.items():
            pattern_type = extract_pattern_type(source)
            if pattern_type in benchmark_patterns:
                prioritized_results.append((number, source))
            else:
                other_results.append((number, source))
        
        final_sorted_output = prioritized_results + other_results
        final_output_list = final_sorted_output[:NUM_RESULTS_TO_OUTPUT]

        # =================================================================
        # FASE 3: MENYIMPAN HASIL
        # =================================================================
        print(f"\n--- FASE 3: Menyimpan Hasil Akhir ---")
        if final_output_list:
            output_filename = f'predicted_numbers_{GAME_CODE}_{len(final_output_list)}.txt'
            with open(output_filename, 'w') as f:
                f.write(f"--- HASIL PREDIKSI UNTUK {GAME_CODE} ---\n\n")
                f.write("--- HASIL DENGAN PRIORITAS BENCHMARK ---\n")
                count_benchmark = 0
                for number, source in prioritized_results:
                     if (number, source) in final_output_list:
                        f.write(f"{number} --> {source} [BENCHMARK]\n")
                        count_benchmark += 1
                
                f.write(f"\n--- HASIL LAINNYA ({len(final_output_list) - count_benchmark}) ---\n")
                for number, source in other_results:
                    if (number, source) in final_output_list:
                        f.write(f"{number} --> {source}\n")

            print(f"Semua {len(final_output_list)} nomor telah disimpan ke '{output_filename}'")
            print("\nContoh Hasil Teratas:")
            for i, (number, source) in enumerate(final_output_list[:15]):
                 is_benchmark = extract_pattern_type(source) in benchmark_patterns
                 print(f"{i+1}. {number} --> {source} {'[BENCHMARK]' if is_benchmark else ''}")
        else:
            print("Tidak ada nomor yang berhasil diprediksi/dihasilkan.")

        print("\nProses selesai!")

    else:
        print("\nProses dihentikan karena tidak ada data yang valid atau data historis tidak cukup.")