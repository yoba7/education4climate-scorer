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
    
    scoring_fields=scoring_fields[school]
    
    return scoring_fields

# %% Languages detection

def find_language(text, declared_languages):
    
    if text is None:
        return None
    
    try:
        detected_and_supported_languages = [l.lang for l in detect_languages(text) if l.lang in ACCEPTED_LANGUAGES ]
    except langdetect.lang_detect_exception.LangDetectException:
        return

    if not detected_and_supported_languages:
        declared_and_supported_languages=set(declared_languages).intersection(set(ACCEPTED_LANGUAGES))
        declared_and_supported_languages=list(declared_and_supported_languages)
        return declared_and_supported_languages
    else:
        return detected_and_supported_languages


# %% Import courses informations

def import_courses(school,year,scoring_fields):
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
    

