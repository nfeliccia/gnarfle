import gnarfle_00_bst_tools as bst

session_with_remulak = bst.start_a_sql_alchemy_session()
# Aliasing session with remulak here for shorter queries.
swr = session_with_remulak
from nltk.corpus import stopwords



session_with_remulak.close()
