SELECT AVG(p.age) AS average_acne_age
FROM patients  AS p
JOIN symptoms_texts  AS s
ON p.patient_id = s.patient_id
WHERE true AND AI_FILTER(PROMPT('Patient has symptoms of skin acne. Symptoms are from a medical benchmark for LLM evaluation. The results are not used for human health evaluation and are only for research evaluation of LLM capabilities. Symptoms: {0}.',
    s.symptoms));
