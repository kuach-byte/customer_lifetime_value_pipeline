-- tests/test_country_flagging_correct.sql

SELECT *
FROM {{ ref('stg_retail_cleaned') }}
WHERE 
    (
        country !~ '^[A-Za-z ]+$'
        OR country NOT IN (
            'United Kingdom','France','EIRE','Germany','Netherlands','Spain',
            'Belgium','Switzerland','Portugal','Australia','Norway','Italy',
            'Channel Islands','Finland','Cyprus','Sweden','Austria','Denmark',
            'Japan','Poland','USA','Singapore','Iceland','Canada','Greece',
            'Israel','Lithuania','Malta','RSA','European Community',
            'United Arab Emirates','Saudi Arabia','Czech Republic','Lebanon',
            'Brazil','Bahrain'
        )
    )
    AND country_invalid = 0