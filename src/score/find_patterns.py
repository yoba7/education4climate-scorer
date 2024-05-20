#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 19 21:14:25 2024

@author: yoba
"""

# %% Imports

import sqlite3
import pandas as pd
import re
import json
import argparse
import langdetect  
from langdetect import detect_langs as detect_languages
from pathlib import Path
from datetime import datetime


ACCEPTED_LANGUAGES=['en','fr','nl']

# %% Import list of fields to score

def import_scoring_fields(school):
    with open(f"{root}/data/scoring_fields.json") as f:
        scoring_fields=json.load(f)   
    return scoring_fields[school]

# %% Languages detection

def find_language(text, declared_languages):
    
    '''
    The scorer can be used to find terms in course descriptions. Terms are 
    detected using regular expressions, which are available in three versions, 
    corresponding to the three languages supported (ie: fr, nl and en). 
    
    Exemple of regular expressions (extract of the patterns.json file):
        
     {
     "pattern_id":3,
     "pattern_en":"bioclimat[^ ]+ architecture",
     "pattern_fr":"architectur[^ ]+ bioclimatique",
     "pattern_nl":"bioklima[^ ]+ architect",
     "theme":"building"
     }

    This function determines the language of the regular expressions to be used 
    to analyse the texts. This language must be a supporte language.
    
    The function analyses the language of the description. If the detection 
    fails (the langdetect package is very efficient but sometimes fails to
    detect the language of a descritpion) then we'll take into 
    account the language(s) of the lesson as declared by the teacher. 
    
    The function only return supported languages (unsupported languages
    are filtered out).
    
    Args:
        text : the text to analyse
        declared_languages : langugage(s) of the lesson as declared by the teacher.
        
    Returns:
        List of languages.

    Examples:
        
    >>> find_language("La descrizione è in italiano. Car il s'agit d'un cours d'italien mais qui se donne aussi en français",['it'])
    ['fr']

    >>> find_language("La descrizione è in italiano.",['it'])
    []

    >>> find_language("The description of the course in in English",['en','fr'])
    ['en']

    >>> find_language("The description of the course in in English",['fr'])
    ['en']
    '''
    
    if text is None:
        return None
    
    try:
        detected_and_supported_languages = [l.lang for l in detect_languages(text) if l.lang in ACCEPTED_LANGUAGES ]
    except langdetect.lang_detect_exception.LangDetectException:
        return None

    if not detected_and_supported_languages:
        declared_and_supported_languages=set(declared_languages).intersection(set(ACCEPTED_LANGUAGES))
        declared_and_supported_languages=list(declared_and_supported_languages)
        return declared_and_supported_languages
    else:
        return detected_and_supported_languages


# %% Import courses informations

def import_courses(school,year,scoring_fields):
    
    '''
    This function imports courses information that have been scraped. 
    
    Remarks:
        - We get rid of some annoying characters that cause some troubles. The 
          process has no effect on the length of texts. 
        - We are also making use of the find_language function to detect 
          languages of course description fields. We create new variables 
          in the resulting DataFrame to store this information.
    
    '''
    
    # Import
    courses = pd.read_json(open(f"{root}/data/crawling-output/{school}_courses_{year}.json", 'r'), 
                           dtype={'id': str})
    
    
    # Get rid of some annonying characters
    translation_map = [    
                         ("\xa0", " "),
                         ("’"   , "'"),
                      ]
        
    translation_map=str.maketrans(''.join([x for (x,y) in translation_map ]),
                                  ''.join([y for (x,y) in translation_map ]))
    
    def clean_text(text: str):
        text=text.translate(translation_map)
        return text
    
    for scoring_field in scoring_fields:
        courses[scoring_field] = courses[scoring_field].apply(clean_text)

    # Detect languages of scoring fields
    for scoring_field in scoring_fields[1:]: 
        courses[f"{scoring_field}_languages4scoring"]=courses.apply(lambda course: find_language(getattr(course,scoring_field,None),course.languages),axis=1)
        
    return courses




# %% Import patterns

def import_patterns():
    patterns=pd.read_json(f'{root}/data/patterns/patterns.json')
    
    # We'll consider tab characters, return characters, ... like blank characters
    def correct(pattern):
        if pattern is not None:
            return pattern.replace('[- ]','[\-\s]')\
                          .replace('[^ ]','[^\s]' )\
                          .replace(' '   ,'\s+'   )
    
    for p in ['pattern_en','pattern_fr','pattern_nl']:
        patterns[p] = patterns[p].apply(correct)
    
    # Split and compile patterns (will simplify subsequent code and make it more performant)
    def break_into_sub_patterns(pattern):
        if pattern is not None:
            return [re.compile(pat, re.IGNORECASE) for pat in pattern.split("#") ]
        else:
            return []
    
    for p in ['pattern_en','pattern_fr','pattern_nl']:
        patterns[p] = patterns[p].apply(break_into_sub_patterns)
    
    # Rename fields (will simplify subsequent code)
    patterns.rename(columns={'pattern_id':'id',
                             'pattern_en':'en',
                             'pattern_fr':'fr',
                             'pattern_nl':'nl'},inplace=True)


    return patterns

# %% Function to find patterns in courses

def find_pattern_in_course_field(course,scoring_field,pattern,db):
    
    text = getattr(course,scoring_field,None)
    if not text: # If text is missing then silently return
        return

    languages = getattr(course,f"{scoring_field}_languages4scoring",None) 
    if not languages: # If text language is not supported then silently return
        return   

    main_language=languages[0]
    
    sub_patterns = getattr(pattern, main_language,None)
    if not sub_patterns:
        return # If there is no sub_pattern, then silently return

    matched_patterns      = []

    for sub_pattern in sub_patterns:

        sub_pattern_matches = list(sub_pattern.finditer(text))

        if not sub_pattern_matches:
            return # If there are no matches for a sub-pattern, stop the search

        for sub_pattern_match in sub_pattern_matches:
            start, end = sub_pattern_match.span()
            matched_patterns +=  [
                                      {
                                       'id'          :course.id                                         ,
                                       'field'       :scoring_field                                     ,
                                       'pattern'     :pattern.id                                        ,
                                       'sub_pattern' :sub_pattern.pattern                               ,
                                       'start'       :start                                             ,
                                       'end'         :end                                               ,
                                       'extract'     :text[max(0, start-20) : min(end+20, len(text)-1)] ,
                                      }
                                  ]

    matched_patterns=pd.DataFrame.from_dict(matched_patterns)
    matched_patterns.to_sql('T_matches',db,if_exists='append')

    return

# %% Score

def score(school,year):
    
    print(f'{datetime.now().isoformat()} - Import scoring field')
    scoring_fields = import_scoring_fields(school)
    
    print(f'{datetime.now().isoformat()} - Import courses + detect languages')
    courses        = import_courses(school,year,scoring_fields)
    
    print(f'{datetime.now().isoformat()} - Import patterns')
    patterns       = import_patterns()
    
    db=sqlite3.connect(':memory:')
    
    ms_countOf_courses,_=courses.shape
    ms_countOf_patterns,_=patterns.shape
    ms_countOf_scoringFields=len(scoring_fields)
    ms_countOf_matchFunctionCalls=ms_countOf_courses*ms_countOf_patterns*ms_countOf_scoringFields
    
    c,s=0,0
    
    for course in courses.itertuples():
        c+=1
        for pattern in patterns.itertuples():
            for field in scoring_fields:
                s+=1
                find_pattern_in_course_field(course,field,pattern,db)
                if s%100000==0:
                    print(f'{datetime.now().isoformat()} - step {s} - course {c}/{ms_countOf_courses} - progress {s/ms_countOf_matchFunctionCalls:.2%}')
    
    # %% Export of results
    
    pd.read_sql('select * from T_matches',db).to_json(f'{root}/data/scorer-output/{school}_{year}.json',indent=5,index=False,orient='records')

# %% Score

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--school", help="School code", default='uclouvain')
    parser.add_argument("-y", "--year", help="Academic year", default=2023)

    arguments = vars(parser.parse_args())
    
    school=arguments['school']
    year=arguments['year']
    
    root=Path(__file__).parent.absolute().joinpath("../..")
    
    print(f'School: {school}')
    print(f'Year: {year}')
    print(f'Root directory: {root}')
    
    print('Process starts ...')
    score(school,year)
    print('Process ends')
    

