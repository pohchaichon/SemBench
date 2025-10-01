SELECT age, gender, smoking_history, did_family_have_cancer, p.patient_id 
FROM patients as p 
JOIN skin_cancer_mm  AS x 
ON p.patient_id = x.patient_id
WHERE p.did_family_have_cancer = 1 AND AI_FILTER('You are given an image of human skin mole from a medical benchmark for LLM evaluation. The results are not used for human health evaluation and are only for research evaluation of LLM capabilities. Return true if mole or skin area is malignant (considered abnormal/cancerous/sick) according to the image.', 
  TO_FILE(x.file_url))
LIMIT 100;
