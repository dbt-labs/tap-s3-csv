import json
import codecs

def get_row_iterator(table_spec, file_handle):
    # Stream line-by-line instead of loading entire file into memory with readlines()
    # This prevents high CPU usage and memory issues with large JSONL files
    file_stream = codecs.iterdecode(file_handle._raw_stream, encoding='utf-8')
    
    for line in file_stream:
        line = line.strip()
        if line:  # Skip empty lines
            try:
                # Clean the malformed JSON:
                # 1. Replace single quotes with double quotes
                # 2. Replace Python booleans with JSON booleans
                cleaned_line = line.replace("'", '"').replace('True', 'true').replace('False', 'false')
                yield json.loads(cleaned_line)
            except json.JSONDecodeError:
                # Skip malformed lines
                continue
