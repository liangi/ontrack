import pandas as pd

from text_cleaners import clean_text
from cosine_matcher import CosineMatcher
from string_searcher import es_find

cos_match = CosineMatcher()

def read(path, keep_columns='', encoding='latin-1'): #all other columns will be discarded
    df = pd.read_csv(path, encoding=encoding, error_bad_lines=False)
    df = df.dropna(axis=0)
    return df
    
def clean_subset(df, cols='', cleaner=clean_text):
    if cols:
        try:
            df = df[cols]
        except KeyError:
            print "At least one of the columns you entered is not in the given database. The whole database will be kept."
    df = df.astype('unicode').apply((lambda x: ' '.join(x)), axis=1)
    if cleaner:
        df = df.apply(cleaner)
    return df

def find_exact(dbA, dbB, colA='', colB='', regex=False, cleaner=clean_text, fname='exact search'):
    dbA = pd.DataFrame(dbA, index=range(len(dbA)))
    dbB = pd.DataFrame(dbB, index=range(len(dbB)))
    if cleaner:
        A = clean_subset(dbA, colA, cleaner)
        B = clean_subset(dbB, colB, cleaner)
    found = A.apply(lambda x: es_find(B, x, regex=regex))
    db = pd.DataFrame([], columns=['nth query'])
    noResult = pd.DataFrame()
    for i in xrange(len(dbA)):
        if not len(found[i]):  
            noResult = noResult.append(dbA.ix[i])
        else:
            temp = dbB.ix[found[i]] # errors if found is empty     
            temp['nth query'] = i
            db = db.append(temp)        
    results = pd.merge(dbA, db, how='inner', left_index=True, right_on='nth query')
    results.to_csv(fname+'.csv', index=False, encoding='utf-8')
    noResult.to_csv(fname+' - no match.csv', index=False, encoding='utf-8')
    return results

def find_closest(dbA, dbB, colA='', colB='', n=5, cleaner=clean_text, fname='closest match'):
    dbA = pd.DataFrame(dbA, index=range(len(dbA)))
    dbB = pd.DataFrame(dbB, index=range(len(dbB)))
    if cleaner:        
        A = clean_subset(dbA, colA, cleaner)
        B = clean_subset(dbB, colB, cleaner)
    
    cos_match.set_corpus(B.values)
    found = A.apply(lambda x: cos_match.find(x, n))
    db = pd.DataFrame()
    for i in xrange(len(found)):
        if len(found[i]['indices']):  
            temp = dbB.ix[found[i]['indices']] # errors if found is empty  
            temp['score'] = found[i]['scores']
            temp['nth query'] = i
            db = pd.concat([db,temp], axis=0)
    results = pd.merge(dbA, db, how='left', left_index=True, right_on='nth query')
    results = results[results.score != 0]
    results.to_csv(fname+'.csv', index=False, encoding='utf-8')
    return results

if __name__ == '__main__':
    pass
