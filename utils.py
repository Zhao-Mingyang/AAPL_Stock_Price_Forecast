import re

stopwords = ['dtd','copyright','llc','en','html','fe','ed','webfilings','e','vk','g','zip code', 'pagebreak','html' 
             'w','c','en','table','body','par','value','per','securities','exchange','comission','telephone','number',
             'zip', 'code', 'end', 'page','xbrl','begin','dc','aa','aaa', 'aaa aa','ab','abn','abn amro','abnormal',
             'abo','abs','ac','az','ba','baa','aoci','aol','apb','api','app','ann','anne','amp','amt','anda','bla','bit',
             'bio','bhc','bb','bbb','bbl','bbls','bc','bcf','bcfe','apr','arc','aro','asa','asa','asc','asic','asp','asr',
             'asu','asus','ave','bms','bnp','bny','boe','blvd','bms','boe','bps','bs','btu', 'btus','ca','cad','cal','ccc',
             'cceeff','cdo','cdos','cds','ce','cede','cg','chk','cmsa','col','com','con','conway','ct','dd','de','dan',
             'dana','dea','wti','wto','wv','wyeth','wyoming','xannual','xerox','xi','xii','xiii','xindicate','xiv','xix','xl',
             'xthe','xv','xvi','xvii','xviii', 'xx','xxi','xxx','wi','vt','vs','von''vie','via','vi','var','ta','tab','tam',
             'td','tdr','tdrs','te','sur','ss','sr','sq','sp','sop','sip','sd','sdn','se']

contractions = {
"ain't": "is not",
"aren't": "are not",
"can't": "cannot",
"can't've": "cannot have",
"'cause": "because",
"could've": "could have",
"couldn't": "could not",
"couldn't've": "could not have",
"didn't": "did not",
"doesn't": "does not",
"don't": "do not",
"hadn't": "had not",
"hadn't've": "had not have",
"hasn't": "has not",
"haven't": "have not",
"he'd": "he would",
"he'd've": "he would have",
"he'll": "he will",
"he'll've": "he he will have",
"he's": "he is",
"how'd": "how did",
"how'd'y": "how do you",
"how'll": "how will",
"how's": "how is",
"I'd": "I would",
"I'd've": "I would have",
"I'll": "I will",
"I'll've": "I will have",
"I'm": "I am",
"I've": "I have",
"i'd": "i would",
"i'd've": "i would have",
"i'll": "i will",
"i'll've": "i will have",
"i'm": "i am",
"i've": "i have",
"isn't": "is not",
"it'd": "it would",
"it'd've": "it would have",
"it'll": "it will",
"it'll've": "it will have",
"it's": "it is",
"let's": "let us",
"ma'am": "madam",
"mayn't": "may not",
"might've": "might have",
"mightn't": "might not",
"mightn't've": "might not have",
"must've": "must have",
"mustn't": "must not",
"mustn't've": "must not have",
"needn't": "need not",
"needn't've": "need not have",
"o'clock": "of the clock",
"oughtn't": "ought not",
"oughtn't've": "ought not have",
"shan't": "shall not",
"sha'n't": "shall not",
"shan't've": "shall not have",
"she'd": "she would",
"she'd've": "she would have",
"she'll": "she will",
"she'll've": "she will have",
"she's": "she is",
"should've": "should have",
"shouldn't": "should not",
"shouldn't've": "should not have",
"so've": "so have",
"so's": "so as",
"that'd": "that would",
"that'd've": "that would have",
"that's": "that is",
"there'd": "there would",
"there'd've": "there would have",
"there's": "there is",
"they'd": "they would",
"they'd've": "they would have",
"they'll": "they will",
"they'll've": "they will have",
"they're": "they are",
"they've": "they have",
"to've": "to have",
"wasn't": "was not",
"we'd": "we would",
"we'd've": "we would have",
"we'll": "we will",
"we'll've": "we will have",
"we're": "we are",
"we've": "we have",
"weren't": "were not",
"what'll": "what will",
"what'll've": "what will have",
"what're": "what are",
"what's": "what is",
"what've": "what have",
"when's": "when is",
"when've": "when have",
"where'd": "where did",
"where's": "where is",
"where've": "where have",
"who'll": "who will",
"who'll've": "who will have",
"who's": "who is",
"who've": "who have",
"why's": "why is",
"why've": "why have",
"will've": "will have",
"won't": "will not",
"won't've": "will not have",
"would've": "would have",
"wouldn't": "would not",
"wouldn't've": "would not have",
"y'all": "you all",
"y'all'd": "you all would",
"y'all'd've": "you all would have",
"y'all're": "you all are",
"y'all've": "you all have",
"you'd": "you would",
"you'd've": "you would have",
"you'll": "you will",
"you'll've": "you will have",
"you're": "you are",
"you've": "you have"
}

def expand_contractions(text):
    for word in text.split():
        if word.lower() in contractions:
            text = text.replace(word, contractions[word.lower()])
    return text


import unicodedata
def remove_accented_chars(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return text


def scrub_words(text):
    text = re.sub('\xa0', ' ', text)
    text = re.sub("(\\W|\\d)",' ',text)
    text = re.sub('\n(\w*?)[\s]', '', text)
    text = re.sub("<.*?>", ' ', text)
    text = re.sub("\s+", ' ', text)
    return text


def review_to_words(raw_review):
    
    remove = re.sub(r'\b\w{1,3}\b', '', raw_review) 
    letters_only = re.sub("[^a-zA-Z]", " ", remove) 
    word = letters_only.lower().split()
  
    meaningful_words = [w for w in word if not w in stopwords] 
    return( " ".join(meaningful_words))