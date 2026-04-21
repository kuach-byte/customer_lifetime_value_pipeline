-- tests/test_country_not_overflagged.sql

SELECT *
FROM {{ ref('stg_retail_cleaned') }}
WHERE 
    country IN (
        'United Kingdom','France','EIRE','Germany','Netherlands','Spain',
        'Belgium','Switzerland','Portugal','Australia','Norway','Italy',
        'Channel Islands','Finland','Cyprus','Sweden','Austria','Denmark',
        'Japan','Poland','USA','Singapore','Iceland','Canada','Greece',
        'Israel','Lithuania','Malta','RSA','European Community',
        'United Arab Emirates','Saudi Arabia','Czech Republic','Lebanon',
        'Brazil','Bahrain'
    )
    AND country ~ '^[A-Za-z ]+$'
    AND country_invalid = 1