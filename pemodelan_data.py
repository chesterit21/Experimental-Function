import pyodbc
import pandas as pd
import numpy as np
import itertools
from collections import Counter
from sklearn.ensemble import RandomForestClassifier # Import kembali untuk pendekatan kedua
from sklearn.metrics import accuracy_score, classification_report # Untuk evaluasi RF

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
NUM_RESULTS_TO_OUTPUT = 7000 # <-- Anda bisa mengubah nilai ini!

# --- Fungsi untuk Mengambil Data ---
def get_log_game_data(server, database, table, conn_str):
    try:
        print(f"Mencoba mengambil data dari tabel '{table}'...")
        cnxn = pyodbc.connect(conn_str)
        # Ambil semua kolom yang relevan, LogResult sebagai string (nvarchar)
        query = f"SELECT Periode, LogResult, [As], Kop, Kepala, Ekor FROM {table} WHERE GameCode='MQ20' ORDER BY Periode ASC"
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
def get_near_logs(df, current_log_result_str, search_type, window_size=1):
    """
    Mencari LogResult yang cocok dengan pola 2 digit tertentu dan mengambil near logs.
    search_type: 'depan', 'tengah', 'belakang'
    window_size: berapa periode sebelum/sesudah yang diambil. Default 1 (sebelum, current, sesudah).
    """
    df_temp = df.copy() # Hindari SettingWithCopyWarning
    df_temp['LogResult_Str_Full'] = df_temp['LogResult'].astype(str).str.zfill(4) # Kolom sementara
    
    search_pattern = ""
    if search_type == 'depan':
        search_pattern = current_log_result_str[:2]
        matching_rows = df_temp[df_temp['LogResult_Str_Full'].str.startswith(search_pattern)]
    elif search_type == 'tengah':
        search_pattern = current_log_result_str[1:3]
        matching_rows = df_temp[df_temp['LogResult_Str_Full'].str[1:3] == search_pattern]
    elif search_type == 'belakang':
        search_pattern = current_log_result_str[2:4]
        matching_rows = df_temp[df_temp['LogResult_Str_Full'].str.endswith(search_pattern)]
    else:
        raise ValueError("search_type harus 'depan', 'tengah', atau 'belakang'")

    near_logs_list = []
    
    df_sorted = df_temp.sort_values(by='Periode', ascending=True).reset_index(drop=True)

    for idx in matching_rows.index:
        original_idx_in_df_sorted = df_sorted[df_sorted['Periode'] == df_temp.loc[idx, 'Periode']].index[0]

        start_idx = max(0, original_idx_in_df_sorted - window_size)
        end_idx = min(len(df_sorted) - 1, original_idx_in_df_sorted + window_size)
        
        near_log_data = []
        for i in range(start_idx, end_idx + 1):
            # Pastikan tidak melampaui batas DataFrame
            if i >= 0 and i < len(df_sorted):
                near_log_data.append({
                    'Periode': df_sorted.loc[i, 'Periode'],
                    'LogResult': df_sorted.loc[i, 'LogResult_Str_Full']
                })
        
        near_logs_list.append({
            'Periode': df_sorted.loc[original_idx_in_df_sorted, 'Periode'],
            'LogResult': df_sorted.loc[original_idx_in_df_sorted, 'LogResult_Str_Full'],
            'NearLog': near_log_data
        })
    
    return near_logs_list

# --- Fungsi untuk Menggenerasi Kandidat Nomor dari NearLog ---
def generate_candidates_from_near_logs(near_logs_data, include_modified=True):
    candidates = []
    
    for entry in near_logs_data:
        for near_log_item in entry['NearLog']:
            current_num_str = near_log_item['LogResult']
            candidates.append(current_num_str) # Tambahkan nomor asli

            if include_modified:
                current_num_list = [int(d) for d in current_num_str]
                
                for i in range(4):
                    original_digit = current_num_list[i]
                    
                    if original_digit < 9:
                        modified_list = list(current_num_list)
                        modified_list[i] = original_digit + 1
                        candidates.append("".join(map(str, modified_list)))
                    
                    if original_digit > 0:
                        modified_list = list(current_num_list)
                        modified_list[i] = original_digit - 1
                        candidates.append("".join(map(str, modified_list)))
                        
    return candidates

# --- Fungsi untuk Menggenerasi Kandidat dari Probabilitas RF ---
def generate_candidates_from_rf_probs(predicted_probs, num_to_generate):
    all_combinations = list(itertools.product(range(10), repeat=4))
    
    combined_probabilities = []
    for combo in all_combinations:
        prob_As = predicted_probs['As'].get(combo[0], 0.0)
        prob_Kop = predicted_probs['Kop'].get(combo[1], 0.0)
        prob_Kepala = predicted_probs['Kepala'].get(combo[2], 0.0)
        prob_Ekor = predicted_probs['Ekor'].get(combo[3], 0.0)
        
        total_prob = prob_As * prob_Kop * prob_Kepala * prob_Ekor
        
        formatted_number = "".join(map(str, combo))
        combined_probabilities.append((formatted_number, total_prob))

    combined_probabilities.sort(key=lambda x: x[1], reverse=True)
    
    # Ambil sejumlah nomor sesuai permintaan, atau semua jika kurang
    return combined_probabilities[:num_to_generate]


# --- Main Program ---
if __name__ == "__main__":
    df_log = get_log_game_data(SERVER_NAME, DATABASE_NAME, TABLE_NAME, CONNECTION_STRING)

    if df_log is not None:
        # Pastikan LogResult di df_log sudah dalam format string 4 digit sejak awal
        df_log['LogResult_Str'] = df_log['LogResult'].astype(str).str.zfill(4)

        # 1. Ambil LogResult Terakhir
        if not df_log.empty:
            last_log_result_data = df_log.sort_values(by='Periode', ascending=False).iloc[0]
            last_log_result_str = last_log_result_data['LogResult_Str']
            last_periode = last_log_result_data['Periode']
            print(f"\nLogResult terakhir (Periode {last_periode}): {last_log_result_str}")
        else:
            print("DataFrame kosong. Tidak dapat melakukan prediksi.")
            exit()

        # --- Pendekatan 1: NearLog ---
        print("\n--- Pendekatan 1: Mencari LogResult NearLog Berdasarkan Pola 2 Digit ---")
        
        df_search_nearlog = df_log[df_log['Periode'] < last_periode].copy()

        print(f"Mencari pattern 2 digit depan ({last_log_result_str[:2]}XX)...")
        near_logs_depan = get_near_logs(df_search_nearlog, last_log_result_str, 'depan')
        print(f"Ditemukan {len(near_logs_depan)} kecocokan untuk 2 digit depan.")

        print(f"Mencari pattern 2 digit tengah (X{last_log_result_str[1:3]}X)...")
        near_logs_tengah = get_near_logs(df_search_nearlog, last_log_result_str, 'tengah')
        print(f"Ditemukan {len(near_logs_tengah)} kecocokan untuk 2 digit tengah.")

        print(f"Mencari pattern 2 digit belakang (XX{last_log_result_str[2:4]})...")
        near_logs_belakang = get_near_logs(df_search_nearlog, last_log_result_str, 'belakang')
        print(f"Ditemukan {len(near_logs_belakang)} kecocokan untuk 2 digit belakang.")

        all_near_logs_data = near_logs_depan + near_logs_tengah + near_logs_belakang
        print(f"\nTotal entri 'NearLog' yang ditemukan: {len(all_near_logs_data)}")

        candidate_numbers_raw_nearlog = generate_candidates_from_near_logs(all_near_logs_data, include_modified=True)
        # Gunakan set untuk mendapatkan angka unik dan simpan frekuensinya (skor)
        frequency_counter_nearlog = Counter(candidate_numbers_raw_nearlog)
        
        # Format ke (nomor, skor) untuk sorting
        nearlog_results = [(num, freq) for num, freq in frequency_counter_nearlog.items()]
        nearlog_results.sort(key=lambda x: x[1], reverse=True)

        # Dapatkan hanya nomor unik dari hasil nearlog
        unique_numbers_from_nearlog = {num for num, _ in nearlog_results}
        
        print(f"Total kandidat nomor unik dari NearLog: {len(unique_numbers_from_nearlog)}")

        # --- Pendekatan 2: Probabilitas Digit dengan Random Forest ---
        print("\n--- Pendekatan 2: Pemodelan Probabilitas Digit dengan Random Forest ---")

        # Pra-pemrosesan Data dan Pembuatan Fitur Lag untuk RF
        df_rf = df_log.copy() # Gunakan salinan DataFrame untuk RF agar tidak mengganggu df_log asli

        # Pastikan kolom As, Kop, Kepala, Ekor adalah integer
        digit_cols = ['As', 'Kop', 'Kepala', 'Ekor']
        for col in digit_cols:
            df_rf[col] = pd.to_numeric(df_rf[col], errors='coerce').fillna(-1).astype(int) # Handle potensial NaN jika ada

        num_lags = 3
        for col in digit_cols:
            for i in range(1, num_lags + 1):
                df_rf[f'{col}_lag{i}'] = df_rf[col].shift(i)
        
        df_rf.dropna(inplace=True)
        
        for col in digit_cols:
            for i in range(1, num_lags + 1):
                df_rf[f'{col}_lag{i}'] = df_rf[f'{col}_lag{i}'].astype(int)

        print(f"Data untuk model Random Forest setelah fitur lag: {df_rf.shape}")

        features_rf = [f'{col}_lag{i}' for col in digit_cols for i in range(1, num_lags + 1)]
        X_rf = df_rf[features_rf]
        
        # Ambil baris terakhir dari data yang sudah di-preprocess untuk RF sebagai input untuk prediksi angka berikutnya
        # Pastikan X_rf tidak kosong
        if X_rf.empty:
            print("Data untuk model Random Forest terlalu sedikit setelah dropna. Tidak dapat melatih model RF.")
            rf_generated_numbers = [] # Set kosong jika RF tidak bisa jalan
        else:
            split_point_rf = int(len(df_rf) * 0.8)
            X_train_rf, X_test_rf = X_rf.iloc[:split_point_rf], X_rf.iloc[split_point_rf:]
            
            models_rf = {}
            latest_data_for_prediction_rf = X_rf.iloc[-1:].copy() 

            predicted_probs_rf = {}
            for digit_col in digit_cols:
                y_rf = df_rf[digit_col]
                y_train_rf, y_test_rf = y_rf.iloc[:split_point_rf], y_rf.iloc[split_point_rf:]

                print(f"  Melatih model Random Forest untuk digit {digit_col}...")
                model_rf = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
                model_rf.fit(X_train_rf, y_train_rf)
                models_rf[digit_col] = model_rf

                y_pred_rf = model_rf.predict(X_test_rf)
                print(f"  Akurasi RF {digit_col} pada data uji: {accuracy_score(y_test_rf, y_pred_rf):.4f}")
                # print(f"  Classification Report untuk {digit_col}:\n{classification_report(y_test_rf, y_pred_rf, zero_division=0)}")
            
            print("Semua model Random Forest dilatih.")

            print("\n  Memprediksi Probabilitas dengan Random Forest...")
            for digit_col in digit_cols:
                model_rf = models_rf[digit_col]
                probs_rf = model_rf.predict_proba(latest_data_for_prediction_rf)[0] 
                
                digit_to_prob_map_rf = {model_rf.classes_[i]: prob for i, prob in enumerate(probs_rf)}
                full_digit_probs_rf = {i: digit_to_prob_map_rf.get(i, 0.0) for i in range(10)}
                predicted_probs_rf[digit_col] = full_digit_probs_rf
            
            # Generasi kandidat dari RF
            rf_generated_numbers_with_probs = generate_candidates_from_rf_probs(predicted_probs_rf, 10000) # Ambil semua 10000 kombinasi
            # Convert to a set of numbers to easily check for uniqueness later
            rf_generated_numbers = {num for num, _ in rf_generated_numbers_with_probs}

        print(f"Total kandidat nomor unik dari Random Forest: {len(rf_generated_numbers)}")

        # --- Menggabungkan Hasil dan Menghasilkan Daftar Final ---
        print("\n--- Menggabungkan Hasil dari Kedua Pendekatan ---")
        
        final_predicted_numbers = []
        
        # Prioritaskan nomor dari NearLog
        for num, freq in nearlog_results:
            final_predicted_numbers.append(num)
            if len(final_predicted_numbers) >= NUM_RESULTS_TO_OUTPUT:
                break
        
        # Jika masih belum mencapai target, tambahkan dari hasil Random Forest (yang paling probable)
        if len(final_predicted_numbers) < NUM_RESULTS_TO_OUTPUT:
            num_needed = NUM_RESULTS_TO_OUTPUT - len(final_predicted_numbers)
            print(f"NearLog menghasilkan {len(final_predicted_numbers)} nomor unik. Memerlukan tambahan {num_needed} dari Random Forest.")
            
            count_added_from_rf = 0
            if 'rf_generated_numbers_with_probs' in locals(): # Pastikan RF model berjalan
                for num, prob in rf_generated_numbers_with_probs:
                    if num not in unique_numbers_from_nearlog: # Tambahkan hanya yang belum ada dari NearLog
                        final_predicted_numbers.append(num)
                        count_added_from_rf += 1
                        if len(final_predicted_numbers) >= NUM_RESULTS_TO_OUTPUT:
                            break
            print(f"Ditambahkan {count_added_from_rf} nomor dari Random Forest.")
        
        # Pastikan tidak melebihi target jika ada sedikit kelebihan
        final_predicted_numbers = final_predicted_numbers[:NUM_RESULTS_TO_OUTPUT]

        print(f"\nTotal nomor unik yang dihasilkan: {len(final_predicted_numbers)}")
        print(f"\n{len(final_predicted_numbers)} Nomor Teratas (Gabungan) yang Diprediksi Memiliki Probabilitas Tinggi:")
        # Tampilkan 10 nomor teratas sebagai contoh
        for i, number in enumerate(final_predicted_numbers[:10]):
            print(f"{i+1}. Nomor: {number}")
        
        # Simpan semua nomor teratas ke file teks
        output_filename = f'predicted_high_probability_numbers_Combined_MQ18_{NUM_RESULTS_TO_OUTPUT}.txt'
        with open(output_filename, 'w') as f:
            for number in final_predicted_numbers:
                f.write(f"{number}\n")
        
        print(f"\nSemua {len(final_predicted_numbers)} nomor teratas telah disimpan ke '{output_filename}'")
        print("Pemodelan dan prediksi selesai!")

    else:
        print("Gagal mendapatkan data, tidak bisa melanjutkan proses.")