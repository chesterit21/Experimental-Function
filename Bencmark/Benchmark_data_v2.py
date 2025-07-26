import pyodbc
import pandas as pd
from collections import Counter
from tqdm import tqdm
import json
import os
import itertools # Pustaka untuk menangani kombinasi

# --- Konfigurasi Utama ---
GAME_CODE = 'NCD'
BENCHMARK_FILENAME = f"{GAME_CODE}_benchmark_patterns.json"

# --- Konfigurasi Koneksi SQL Server ---
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
NUM_RESULTS_TO_OUTPUT = 8900
BACKTEST_TARGET_COUNT = 9400
NUM_BENCHMARK_PATTERNS = 15
INCREMENTAL_BACKTEST_DAYS = 14
PERIODS_PER_DAY = 1
INCREMENTAL_BACKTEST_PERIODS = INCREMENTAL_BACKTEST_DAYS * PERIODS_PER_DAY

# --- Fungsi Manajemen Data & Benchmark ---
def get_log_game_data(server, database, table, conn_str, game_code):
    try:
        print(f"Mencoba mengambil data untuk GameCode '{game_code}'...")
        cnxn = pyodbc.connect(conn_str)
        query = f"SELECT Periode, LogResult FROM {table} WHERE GameCode='{game_code}' ORDER BY Periode ASC"
        df = pd.read_sql(query, cnxn)
        if df.empty:
            print(f"Peringatan: Tidak ada data untuk GameCode '{game_code}'.")
            return None
        df['LogResult_Str'] = df['LogResult'].astype(str).str.zfill(4)
        print(f"Data berhasil diambil! Total {len(df)} periode.")
        return df
    except pyodbc.Error as ex:
        print(f"Kesalahan SQL: {ex.args[1]}")
        return None

def save_benchmark_patterns(filename, patterns):
    try:
        with open(filename, 'w') as f: json.dump(patterns, f, indent=4)
        print(f"Pola benchmark disimpan/diperbarui ke '{filename}'")
    except Exception as e:
        print(f"Gagal menyimpan benchmark: {e}")

def load_benchmark_patterns(filename):
    if not os.path.exists(filename): return None
    try:
        with open(filename, 'r') as f:
            patterns = json.load(f)
            return patterns if isinstance(patterns, list) and patterns else None
    except Exception as e:
        print(f"Gagal memuat benchmark: {e}")
        return None

# --- Fungsi Analisis dan Generasi ---
def extract_pattern_type(source_string):
    if not isinstance(source_string, str): return "Sumber Tidak Diketahui"
    parts = source_string.split()
    # Logika ini tetap berfungsi karena kita masih mencari kata kunci yang sama
    if source_string.startswith("ekspansi kombinatorial level"): return f"Ekspansi Level {parts[3]}"
    if source_string.startswith("dari analisa"): return f"Analisa {parts[2]} Digit {parts[3]}"
    if source_string.startswith("mix dari"): return "Mix"
    return source_string

def add_near_log_candidates(predictions_dict, df, last_log, search_type, num_digits, window=1):
    source_prefix = f"dari analisa {num_digits} Digit"
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

### [KODE DIPERBARUI] ###
def expand_candidates_iteratively(base_dict, target_count):
    final_dict = base_dict.copy()
    source_keys = list(base_dict.keys())
    level = 1
    
    while len(final_dict) < target_count:
        print(f"  Ekspansi Kombinatorial Level {level} dimulai...")
        newly_gen_in_level = {}
        
        for num_str in source_keys:
            if len(final_dict) + len(newly_gen_in_level) >= target_count:
                break

            num_list = [int(d) for d in num_str]
            all_digit_possibilities = []
            
            for digit_char in num_str:
                original_digit = int(digit_char)
                single_digit_options = {original_digit}
                for mod in range(1, level + 1):
                    single_digit_options.add(original_digit + mod)
                    single_digit_options.add(original_digit - mod)
                
                valid_options = sorted([opt for opt in single_digit_options if 0 <= opt <= 9])
                all_digit_possibilities.append(valid_options)

            for combo_tuple in itertools.product(*all_digit_possibilities):
                new_num = "".join(map(str, combo_tuple))
                
                if new_num == num_str:
                    continue
                if new_num not in final_dict and new_num not in newly_gen_in_level:
                    
                    # --- [LOGIKA BARU] Membuat string sumber yang detail ---
                    # Hitung modifikasi untuk setiap digit
                    mods = [new_digit - old_digit for new_digit, old_digit in zip(combo_tuple, num_list)]
                    # Format string modifikasi, contoh: (+1,-1,0,+2)
                    mods_str = ",".join(f"{m:+d}" for m in mods)
                    
                    newly_gen_in_level[new_num] = f"ekspansi kombinatorial level {level} dari {num_str} ({mods_str})"
                    # --- [AKHIR LOGIKA BARU] ---

        if not newly_gen_in_level:
            print("  Ekspansi level ini tidak menghasilkan kandidat baru. Menghentikan proses.")
            break
            
        final_dict.update(newly_gen_in_level)
        level += 1
        
    return final_dict

def generate_full_prediction_set(historical_df, last_log_result_str, target_count):
    predictions = {}
    for nd in [2, 3]:
        for st in ['depan', 'tengah', 'belakang']:
            add_near_log_candidates(predictions, historical_df, last_log_result_str, st, nd)
    if len(predictions) < target_count:
        predictions = expand_candidates_iteratively(predictions, target_count)
    return predictions

def generate_mixed_candidates(number_str):
    if len(number_str) != 4: return []
    variations = set()
    n = number_str
    variations.add(n[2:] + n[:2]); variations.add(n[1:] + n[0]); variations.add(n[:2] + n[3] + n[2])
    variations.add(n[3] + n[2] + n[:2]); variations.add(n[1] + n[0] + n[2:])
    variations.discard(number_str)
    return list(variations)

# --- Program Utama ---
if __name__ == "__main__":
    df_log = get_log_game_data(SERVER_NAME, DATABASE_NAME, TABLE_NAME, CONNECTION_STRING, GAME_CODE)

    if df_log is not None and len(df_log) > 1:
        # FASE 1: BACKTESTING & MANAJEMEN BENCHMARK
        print(f"\n--- FASE 1: Memeriksa dan Menjalankan Backtesting untuk '{GAME_CODE}' ---")
        existing_patterns = load_benchmark_patterns(BENCHMARK_FILENAME)
        pattern_benchmark_counter = Counter()
        backtest_start_index = 1
        if existing_patterns:
            print(f"File benchmark '{BENCHMARK_FILENAME}' ditemukan. Memuat {len(existing_patterns)} pola.")
            pattern_benchmark_counter.update(existing_patterns)
            backtest_start_index = max(1, len(df_log) - INCREMENTAL_BACKTEST_PERIODS)
            print(f"Akan menjalankan backtest inkremental untuk ~{INCREMENTAL_BACKTEST_DAYS} hari terakhir (mulai dari indeks {backtest_start_index})...")
        else:
            print(f"File benchmark '{BENCHMARK_FILENAME}' tidak ditemukan. Menjalankan backtest penuh...")

        for i in tqdm(range(backtest_start_index, len(df_log) - 1), desc=f"Backtesting {GAME_CODE}"):
            historical_data_for_test = df_log.iloc[:i]
            predictions_for_test = generate_full_prediction_set(historical_data_for_test, df_log.iloc[i-1]['LogResult_Str'], BACKTEST_TARGET_COUNT)
            actual_result = df_log.iloc[i]['LogResult_Str']
            if actual_result in predictions_for_test:
                pattern_type = extract_pattern_type(predictions_for_test[actual_result])
                pattern_benchmark_counter[pattern_type] += 1
        
        print("\n--- Hasil Backtesting & Pembaruan Benchmark ---")
        if not pattern_benchmark_counter:
            print("Tidak ada pola benchmark yang ditemukan.")
            benchmark_patterns = []
        else:
            benchmark_patterns = [p for p, c in pattern_benchmark_counter.most_common(NUM_BENCHMARK_PATTERNS)]
            print(f"{len(benchmark_patterns)} Pola Benchmark Teratas (setelah pembaruan):")
            for i, pattern in enumerate(benchmark_patterns): print(f"{i+1}. '{pattern}' (Total Poin: {pattern_benchmark_counter[pattern]})")
            save_benchmark_patterns(BENCHMARK_FILENAME, benchmark_patterns)

        # FASE 2: PREDIKSI FINAL DENGAN LOGIKA MIX & EKSPANSI
        print(f"\n--- FASE 2: Membuat Prediksi Final untuk '{GAME_CODE}'---")
        final_last_log = df_log.iloc[-1]['LogResult_Str']
        print(f"LogResult terakhir untuk prediksi: {final_last_log}")

        # 1. Hasilkan prediksi awal
        final_predictions_dict = {}
        for nd in [2, 3]:
            for st in ['depan', 'tengah', 'belakang']: add_near_log_candidates(final_predictions_dict, df_log, final_last_log, st, nd)
        print(f"Dihasilkan {len(final_predictions_dict)} kandidat awal dari NearLog.")

        # 2. Terapkan logika MIX
        mixed_predictions = {}
        for number, source in list(final_predictions_dict.items()):
            for variation in generate_mixed_candidates(number):
                if variation not in final_predictions_dict and variation not in mixed_predictions:
                    mixed_predictions[variation] = f"mix dari {number}"
        
        # 3. Gabungkan hasil
        final_predictions_dict.update(mixed_predictions)
        print(f"Total kandidat setelah di-mix: {len(final_predictions_dict)}")

        # 4. Terapkan Ekspansi Kombinatorial jika perlu
        if len(final_predictions_dict) < NUM_RESULTS_TO_OUTPUT:
            print(f"Jumlah kandidat ({len(final_predictions_dict)}) kurang dari target. Menjalankan ekspansi kombinatorial...")
            final_predictions_dict = expand_candidates_iteratively(final_predictions_dict, NUM_RESULTS_TO_OUTPUT)
            print(f"Total kandidat setelah ekspansi: {len(final_predictions_dict)}")
        
        # 5. Urutkan hasil berdasarkan benchmark
        prioritized_results = []; other_results = []
        final_output_list_temp = list(final_predictions_dict.items())[:NUM_RESULTS_TO_OUTPUT]
        for number, source in final_output_list_temp:
            if extract_pattern_type(source) in benchmark_patterns:
                prioritized_results.append((number, source))
            else:
                other_results.append((number, source))
        final_output_list = prioritized_results + other_results

        # FASE 3: MENYIMPAN HASIL
        print(f"\n--- FASE 3: Menyimpan Hasil Akhir ---")
        if final_output_list:
            # [LOGIKA BARU] Tentukan folder output dan buat jika belum ada
            output_dir = "Data_Result"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"Folder '{output_dir}' berhasil dibuat.")

            # Buat nama file dan gabungkan dengan path folder
            base_filename = f'pnm_benchmarked_{GAME_CODE}_{len(final_output_list)}.txt'
            output_filename = os.path.join(output_dir, base_filename)
            with open(output_filename, 'w') as f:
                f.write(f"--- HASIL PREDIKSI (MIX & BENCHMARKED) UNTUK {GAME_CODE} ---\n\n")
                f.write("--- HASIL DENGAN PRIORITAS BENCHMARK ---\n")
                for number, source in prioritized_results:
                     if (number, source) in final_output_list: f.write(f"{number} --> {source} [BENCHMARK]\n")
                f.write(f"\n--- HASIL LAINNYA ---\n")
                for number, source in other_results:
                    if (number, source) in final_output_list: f.write(f"{number} --> {source}\n")
            print(f"✅ Semua {len(final_output_list)} nomor telah disimpan ke '{output_filename}'.")
        else: print("Tidak ada nomor yang berhasil diprediksi/dihasilkan.")
        print("\nProses selesai!")
    else: print("\nProses dihentikan karena tidak ada data yang valid.")