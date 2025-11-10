from flask import Flask, request, send_from_directory, jsonify, render_template
import os
import pandas as pd
from rapidfuzz import fuzz
from dateutil import parser
import re

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.path.join(os.getcwd(), 'uploads')
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
app.config['MAX_CONTENT_LENGTH'] =  100* 1024 * 1024  # 50MB max
ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def extract_field(msg, pattern):
    match = re.search(pattern, msg)
    return match.group(1).strip() if match else ''

def extract_body(msg):
    parts = msg.split("\n\n", 1)
    return parts[1].strip() if len(parts) > 1 else ''

def parse_email_from_message_column(row):
    msg = row.get('message', '')
    return {
        'From': extract_field(msg, r'From: (.+)'),
        'To': extract_field(msg, r'To: (.+)'),
        'CC': extract_field(msg, r'Cc: (.+)'),
        'BCC': extract_field(msg, r'Bcc: (.+)'),
        'Subject': extract_field(msg, r'Subject: (.+)'),
        'Date': extract_field(msg, r'Date: (.+)'),
        'Body': extract_body(msg)
    }

def parse_email_from_columns(row):
    return {
        'From': row.get('From') or row.get('from') or '',
        'To': row.get('To') or row.get('to') or '',
        'CC': row.get('CC') or row.get('cc') or '',
        'BCC': row.get('BCC') or row.get('bcc') or '',
        'Subject': row.get('Subject') or row.get('subject') or '',
        'Date': row.get('Date') or row.get('date') or '',
        'Body': row.get('Body') or row.get('body') or row.get('Message') or ''
    }

def get_participants(row):
    people = set()
    for col in ['From', 'To', 'CC', 'BCC']:
        if pd.notna(row[col]) and row[col].strip():
            people.update(map(str.strip, str(row[col]).lower().split(',')))
    return ','.join(sorted(people))

def try_parse_date(date_str):
    try:
        return parser.parse(date_str)
    except:
        return pd.NaT

def preprocess_and_thread(filepath):
    df = pd.read_csv(filepath)
    df.dropna(how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    columns_lower = [col.lower() for col in df.columns]

    if 'message' in columns_lower and len(df.columns) <= 3:
        parsed_df = df.apply(parse_email_from_message_column, axis=1, result_type='expand')
    elif any(col in columns_lower for col in ['from', 'sender']) and 'subject' in columns_lower:
        parsed_df = df.apply(parse_email_from_columns, axis=1, result_type='expand')
    else:
        return None, "CSV must contain either a 'message' column or standard email fields."

    parsed_df.dropna(how='all', inplace=True)
    parsed_df.drop_duplicates(subset=['From', 'To', 'Subject', 'Date', 'Body'], keep='first', inplace=True)

    parsed_df['Normalized_Subject'] = parsed_df['Subject'].fillna('').str.lower().str.replace(r'^(re:|fwd:|fw:)\s*', '', regex=True)
    parsed_df['Participant_Signature'] = parsed_df.apply(get_participants, axis=1)
    parsed_df['bucket_key'] = parsed_df['Normalized_Subject'].str[:5]
    parsed_df['Hybrid_Thread_ID'] = -1
    parsed_df['Datetime'] = parsed_df['Date'].apply(try_parse_date)

    thread_id_counter = 0
    for bucket, group in parsed_df.groupby('bucket_key'):
        group = group.reset_index()
        assigned = [False] * len(group)
        for i, row in group.iterrows():
            if assigned[i]: continue
            thread_id_counter += 1
            base_subject = row['Normalized_Subject']
            base_participants = row['Participant_Signature']
            parsed_df.at[row['index'], 'Hybrid_Thread_ID'] = thread_id_counter
            assigned[i] = True
            for j in range(i + 1, len(group)):
                if assigned[j]: continue
                subj_j = group.at[j, 'Normalized_Subject']
                part_j = group.at[j, 'Participant_Signature']
                if fuzz.token_sort_ratio(base_subject, subj_j) >= 90 and fuzz.token_sort_ratio(base_participants, part_j) >= 80:
                    parsed_df.at[group.at[j, 'index'], 'Hybrid_Thread_ID'] = thread_id_counter
                    assigned[j] = True
    return parsed_df, None

def score_email(row, thread, main_participant):
    score = 0
    if pd.notna(row['Datetime']) and row['Datetime'] == thread['Datetime'].max(): score += 3
    if len(str(row['Body'])) > thread['Body'].dropna().map(len).median(): score += 2
    if main_participant and row['From'] == main_participant: score += 1
    if any(x in str(row['Body']) for x in ['\n-', '\n*', '\n1.']): score += 1
    if '>' in str(row['Body']) or 'original message' in str(row['Body']).lower(): score += 1
    return score

def find_inclusive_email(thread):
    main_participant = thread['From'].mode().iloc[0] if not thread['From'].mode().empty else None
    thread = thread.copy()
    thread['Score'] = thread.apply(lambda r: score_email(r, thread, main_participant), axis=1)
    inclusive = thread.loc[thread['Score'].idxmax()].copy()
    inclusive['Hybrid_Thread_ID'] = thread['Hybrid_Thread_ID'].iloc[0]
    return inclusive

@app.route('/')
def home():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload():
    if 'emailFile' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['emailFile']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if not allowed_file(file.filename):
        return jsonify({"error": "Only CSV files allowed"}), 400

    filename = file.filename
    raw_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(raw_path)

    parsed_df, error = preprocess_and_thread(raw_path)
    if error:
        return jsonify({"error": error}), 400

    processed_filename = 'threaded_' + filename
    processed_path = os.path.join(app.config['UPLOAD_FOLDER'], processed_filename)
    parsed_df.to_csv(processed_path, index=False)

    return jsonify({
        "file_url": f"/uploads/{filename}",
        "processed_url": f"/uploads/{processed_filename}"
    })

@app.route('/view_threads')
def view_threads():
    latest_file = None
    for f in sorted(os.listdir(app.config['UPLOAD_FOLDER']), reverse=True):
        if f.startswith("threaded_") and f.endswith(".csv"):
            latest_file = os.path.join(app.config['UPLOAD_FOLDER'], f)
            break

    if not latest_file:
        return "No threaded file found.", 404

    df = pd.read_csv(latest_file)
    if 'Datetime' not in df.columns:
        df['Datetime'] = df['Date'].apply(try_parse_date)

    inclusives_df = df.groupby('Hybrid_Thread_ID', group_keys=False).apply(find_inclusive_email)
    inclusives = {row['Hybrid_Thread_ID']: row.to_dict() for _, row in inclusives_df.iterrows()}

    threads = []
    for thread_id, group in df.groupby('Hybrid_Thread_ID'):
        group = group.sort_values('Datetime')
        threads.append({"thread_id": thread_id, "emails": group.to_dict('records')})

    return render_template('threads.html', threads=threads, inclusives=inclusives)

@app.route('/uploads/<path:filename>')
def serve_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
    app.run(debug=True)