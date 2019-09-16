from nltk import word_tokenize, ngrams

import gnarfle_module_bst as bst


connection_with_remulak = bst.start_a_sql_alchemy_session()



# print(f'Finished search for job description textat {(time.time() - program_start) * 1000} ')
# print(f'started job text tokenization {(time.time() - program_start) * 1000} ')
job_text_tokenize = [word.lower() for word in word_tokenize(job_text_raw) if word.isalpha()]
print(f'finished job text tokenization {(time.time() - program_start) * 1000} ')
job_text_ngrams = []
print(f'fStarted ngramming {(time.time() - program_start) * 1000} ')
for phrase_length in range(1, 4):
    job_text_ngrams.extend(list(ngrams(job_text_tokenize, phrase_length)))
print(f'Ended ngramming {(time.time() - program_start) * 1000} ')
print(f'started making job string list {(time.time() - program_start) * 1000} ')
job_text_string_list = [' '.join(word_tuple) for word_tuple in job_text_ngrams]
print(f'finsihed making job string list {(time.time() - program_start) * 1000} ')
print(f'Started  counting  {(time.time() - program_start) * 1000} ')
word_dictionary = Counter(job_text_string_list)
print(f'Finished Counting   {(time.time() - program_start) * 1000} ')
