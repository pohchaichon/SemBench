SELECT AI_AGG(
    text,
    'Who has played a role in all the selected movies? Simply give the name of the actor.'
) AS "_output"
FROM lizzy_caplan_text_data
WHERE title IN (
    'Love Is the Drug',
    'Crashing',
    'Cloverfield',
    'My Best Friend''s Girl',
    'Hot Tub Time Machine',
    'The Last Rites of Ransom Pride',
    'Save the Date',
    'Bachelorette',
    '3, 2, 1... Frankie Go Boom',
    'Queens of Country',
    'Item 47',
    'The Night Before',
    'Now You See Me 2',
    'Allied',
    'Extinction',
    'Cobweb'
);
