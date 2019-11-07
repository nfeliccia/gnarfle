import time
from collections import Counter

# and the natural language processing jawn.
import nltk
# For the SQL Alchemy Stuff
from sqlalchemy import MetaData, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Project wide tools
import blunt_skull_tools as bst


def parse_description_to_words_and_count():

    db_string = bst.create_pg_login_string()
    db_six_cyl_engine = create_engine(db_string, echo=False)
    conexion = db_six_cyl_engine.connect()
    # use SQL Alchemy to pull the metadata for the database down.
    metadata = MetaData(db_six_cyl_engine)
    # Declare the class to mirror the table structure.
    Base = declarative_base()
    Base.metadata.create_all(db_six_cyl_engine)

    # Initiate a session based on the engine.
    Session = sessionmaker()
    session_with_remulak = Session(bind=db_six_cyl_engine)

    # perform the query to get the unprocessed textizes.
    yet_to_be_parsed_group = session_with_remulak.query(bst.Text_Process).filter_by(jd_processed=False).order_by('tpq_pk')
    loop_start_time = time.time()
    for yet_to_be_parsed in yet_to_be_parsed_group[0:10]:
        # split it on the new lines because sentence tokenizer doen't do well on line breaks.
        working_jd = yet_to_be_parsed.job_description.split('\n')
        # Tokenize each phrase - problem is that it comes out as a list.
        working_jd_tokenize = [nltk.tokenize.sent_tokenize(phrase) for phrase in working_jd]
        # take the elements which are lists from the previous iteration and flatten them out so each element
        # has one sentence.
        working_jd_sentences = []  # initialize
        for sentence_list in working_jd_tokenize:
            working_jd_sentences.extend(sentence_list)
        # initialize the list of the job search corpus which will be turned into a dictionary.
        job_search_corpus = []
        # create a stemmer class
        stemmer = nltk.stem.porter.PorterStemmer()
        for sentence in working_jd_sentences:
            # note it if word.isalpha() removes punctuation.
            # the iterable here is the tokenizaiton of the working sentence
            word_list = [word.lower() for word in nltk.word_tokenize(sentence) if word.isalpha()]
            for phrase_length in range(1, min(3, len(word_list))):
                for start_word in range(0, len(word_list) - (phrase_length - 1)):
                    phrase_to_add = ' '.join(word_list[start_word:(start_word + phrase_length)])
                    if phrase_length == 1 and stemmer.stem(phrase_to_add) != phrase_to_add:
                        job_search_corpus.append(stemmer.stem(phrase_to_add))
                    job_search_corpus.append(phrase_to_add)
        job_search_corpus_count = Counter(job_search_corpus)
        print(f"Processing phrases{yet_to_be_parsed.isr_pk} {time.time()-loop_start_time}")
        for item in job_search_corpus_count.items():
            this_corpus_dict = bst.Porpus_dictinary()
            this_corpus_dict.isr_pk = yet_to_be_parsed.isr_pk
            this_corpus_dict.phrase = item[0]
            this_corpus_dict.phrase_count = item[1]
            session_with_remulak.add(this_corpus_dict)
            yet_to_be_parsed.jd_processed = True
            session_with_remulak.flush()

        print(f'{yet_to_be_parsed.isr_pk} parsed {time.time()-loop_start_time}')
    print("Start Upload",time.time()-loop_start_time)
    session_with_remulak.commit()
    session_with_remulak.close()
    print ("Finish Upload" ,time.time() - loop_start_time)
