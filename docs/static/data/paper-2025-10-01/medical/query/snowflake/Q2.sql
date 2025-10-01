SELECT DISTINCT p.patient_id 
FROM patients  AS p
JOIN audio_mm  AS a 
ON p.patient_id = a.patient_id
WHERE p.smoking_history != 'Current' AND 
  AI_FILTER('You are given an audio recording of human lungs from a medical benchmark for LLM evaluation. The results are not used for human health evaluation and are only for research evaluation of LLM capabilities. Return true if the recording is healthy lungs, without diseases.', 
  TO_FILE(a.file_url));
