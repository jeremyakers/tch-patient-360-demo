"""
Texas Children's Hospital Patient 360 PoC - Clinical Notes Generator

This module generates realistic synthetic clinical documentation including
progress notes, discharge summaries, radiology reports, and other unstructured
text data that would be found in a pediatric hospital's Epic EHR system.
"""

import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import json
from faker import Faker

class ClinicalNotesGenerator:
    """Generate realistic clinical documentation for pediatric patients."""
    
    def __init__(self, seed: int = 42):
        """Initialize generator with consistent seed for reproducible data."""
        random.seed(seed)
        self.fake = Faker()
        Faker.seed(seed)
        
        # Clinical note templates and components
        self.note_types = [
            'Progress Note', 'Admission Note', 'Discharge Summary', 
            'Consultation Note', 'Nursing Note', 'Emergency Department Note',
            'Procedure Note', 'Follow-up Note'
        ]
        
        self.pediatric_symptoms = [
            'fever', 'cough', 'vomiting', 'diarrhea', 'abdominal pain', 'headache',
            'sore throat', 'ear pain', 'rash', 'congestion', 'wheezing', 'fatigue',
            'poor feeding', 'irritability', 'difficulty breathing', 'shortness of breath',
            'chest pain', 'joint pain', 'muscle aches', 'nausea', 'dizziness',
            'anxiety', 'panic', 'sleep disturbance', 'appetite loss'
        ]
        
        self.physical_exam_findings = {
            'general': ['alert', 'responsive', 'well-appearing', 'ill-appearing', 'anxious', 'comfortable'],
            'vital_signs': ['stable', 'normal for age', 'elevated temperature', 'tachycardic', 'tachypneic'],
            'heent': ['normocephalic', 'atraumatic', 'pupils equal and reactive', 'TMs clear', 'throat erythematous'],
            'cardiovascular': ['regular rate and rhythm', 'no murmurs', 'good perfusion', 'normal S1 S2'],
            'respiratory': ['clear to auscultation', 'good air movement', 'no wheezes', 'no rales', 'symmetric expansion'],
            'abdomen': ['soft', 'non-tender', 'non-distended', 'normal bowel sounds', 'no organomegaly'],
            'extremities': ['no edema', 'full range of motion', 'no deformity', 'good strength'],
            'neurologic': ['alert and oriented', 'no focal deficits', 'cranial nerves intact', 'reflexes normal'],
            'skin': ['warm and dry', 'no rash', 'good turgor', 'no lesions']
        }
        
        self.assessment_plans = {
            'J45.9': {  # Asthma
                'assessment': 'Asthma exacerbation',
                'plan': [
                    'Continue albuterol inhaler 2 puffs every 4-6 hours as needed',
                    'Start/continue inhaled corticosteroid therapy',
                    'Follow up with pulmonology in 2-4 weeks',
                    'Return to ED if worsening symptoms',
                    'Asthma action plan reviewed with family'
                ]
            },
            'F90.9': {  # ADHD
                'assessment': 'Attention deficit hyperactivity disorder',
                'plan': [
                    'Continue current medication regimen',
                    'Behavioral therapy referral',
                    'School accommodations discussed',
                    'Follow up in 3 months',
                    'Monitor growth and development'
                ]
            },
            'E10.9': {  # Type 1 Diabetes
                'assessment': 'Type 1 diabetes mellitus',
                'plan': [
                    'Continue insulin per sliding scale',
                    'Blood glucose monitoring 4x daily',
                    'Endocrinology follow-up in 3 months',
                    'Nutrition counseling',
                    'Annual ophthalmology exam'
                ]
            },
            'J06.9': {  # Upper respiratory infection
                'assessment': 'Viral upper respiratory infection',
                'plan': [
                    'Supportive care with rest and fluids',
                    'Acetaminophen or ibuprofen for fever',
                    'Saline nasal drops for congestion',
                    'Return if symptoms worsen or persist >10 days',
                    'No antibiotics indicated'
                ]
            }
        }
        
        self.radiology_findings = {
            'chest_xray': [
                'lungs are clear bilaterally',
                'no acute cardiopulmonary process',
                'heart size normal for age',
                'no pneumonia or pneumothorax',
                'costophrenic angles are sharp'
            ],
            'abdominal_xray': [
                'normal bowel gas pattern',
                'no obstruction or perforation',
                'no abnormal calcifications',
                'normal organ contours',
                'no free air'
            ],
            'brain_mri': [
                'no acute intracranial abnormality',
                'normal brain parenchyma',
                'no mass effect or midline shift',
                'normal ventricular system',
                'no abnormal enhancement'
            ]
        }
        
        self.nursing_observations = [
            'Patient tolerated procedure well',
            'Vital signs stable throughout shift',
            'Intermittent tachycardia noted while ambulating',
            'Low-grade fever responded to acetaminophen',
            'O2 saturation 92-95% on room air; encouraged deep breathing exercises',
            'Mild wheezing heard; albuterol nebulizer administered with good effect',
            'Patient appears anxious; reassurance provided and parent at bedside',
            'Crying/irritable at times; comfort measures provided',
            f"Pain reported as {random.randint(1, 8)}/10; PRN analgesic given with relief",
            'Family at bedside and supportive',
            'Patient interactive and playful',
            'Appetite fair; taking PO with encouragement',
            'No nausea or vomiting',
            'Voiding normally',
            'Following commands appropriately'
        ]
    
    def generate_progress_note(self, patient_data: Dict, encounter_data: Dict, diagnosis_data: List[Dict]) -> Dict:
        """Generate a pediatric progress note."""
        
        # Select primary diagnosis for note focus
        primary_dx = diagnosis_data[0] if diagnosis_data else None
        dx_code = primary_dx['diagnosis_code'] if primary_dx else 'Z00.129'
        
        # Generate age-appropriate content
        age = self._calculate_age(patient_data['date_of_birth'])
        
        note_content = self._build_progress_note_content(
            patient_data, encounter_data, diagnosis_data, age
        )
        
        return {
            'note_id': f"NOTE-{uuid.uuid4().hex[:8].upper()}",
            'patient_id': patient_data['patient_id'],
            'encounter_id': encounter_data['encounter_id'],
            'note_type': 'Progress Note',
            'note_date': encounter_data['encounter_date'],
            'author': encounter_data['attending_physician'],
            'department': encounter_data['department'],
            'note_content': note_content,
            'diagnosis_codes': [dx['diagnosis_code'] for dx in diagnosis_data],
            'created_date': encounter_data['encounter_date'],
            'updated_date': datetime.now()
        }
    
    def generate_discharge_summary(self, patient_data: Dict, encounter_data: Dict, 
                                 diagnosis_data: List[Dict], medications: List[Dict]) -> Dict:
        """Generate a discharge summary."""
        
        age = self._calculate_age(patient_data['date_of_birth'])
        
        note_content = self._build_discharge_summary_content(
            patient_data, encounter_data, diagnosis_data, medications, age
        )
        
        return {
            'note_id': f"NOTE-{uuid.uuid4().hex[:8].upper()}",
            'patient_id': patient_data['patient_id'],
            'encounter_id': encounter_data['encounter_id'],
            'note_type': 'Discharge Summary',
            'note_date': encounter_data['discharge_date'] or encounter_data['encounter_date'],
            'author': encounter_data['attending_physician'],
            'department': encounter_data['department'],
            'note_content': note_content,
            'diagnosis_codes': [dx['diagnosis_code'] for dx in diagnosis_data],
            'created_date': encounter_data['discharge_date'] or encounter_data['encounter_date'],
            'updated_date': datetime.now()
        }
    
    def generate_radiology_report(self, patient_data: Dict, encounter_data: Dict, 
                                study_type: str) -> Dict:
        """Generate a radiology report."""
        
        age = self._calculate_age(patient_data['date_of_birth'])
        
        note_content = self._build_radiology_report_content(
            patient_data, encounter_data, study_type, age
        )
        
        return {
            'note_id': f"RAD-{uuid.uuid4().hex[:8].upper()}",
            'patient_id': patient_data['patient_id'],
            'encounter_id': encounter_data['encounter_id'],
            'note_type': f'{study_type.replace("_", " ").title()} Report',
            'note_date': encounter_data['encounter_date'] + timedelta(hours=random.randint(1, 6)),
            'author': f"Dr. {self.fake.last_name()}, MD (Radiology)",
            'department': 'Radiology',
            'note_content': note_content,
            'study_type': study_type,
            'created_date': encounter_data['encounter_date'],
            'updated_date': datetime.now()
        }
    
    def generate_nursing_note(self, patient_data: Dict, encounter_data: Dict) -> Dict:
        """Generate a nursing note."""
        
        note_content = self._build_nursing_note_content(patient_data, encounter_data)
        
        return {
            'note_id': f"NURS-{uuid.uuid4().hex[:8].upper()}",
            'patient_id': patient_data['patient_id'],
            'encounter_id': encounter_data['encounter_id'],
            'note_type': 'Nursing Note',
            'note_date': encounter_data['encounter_date'] + timedelta(hours=random.randint(2, 12)),
            'author': f"{self.fake.first_name()} {self.fake.last_name()}, RN",
            'department': encounter_data['department'],
            'note_content': note_content,
            'created_date': encounter_data['encounter_date'],
            'updated_date': datetime.now()
        }
    
    def generate_consultation_note(self, patient_data: Dict, encounter_data: Dict, 
                                 specialty: str, diagnosis_data: List[Dict]) -> Dict:
        """Generate a specialty consultation note."""
        
        age = self._calculate_age(patient_data['date_of_birth'])
        
        note_content = self._build_consultation_note_content(
            patient_data, encounter_data, specialty, diagnosis_data, age
        )
        
        return {
            'note_id': f"CONS-{uuid.uuid4().hex[:8].upper()}",
            'patient_id': patient_data['patient_id'],
            'encounter_id': encounter_data['encounter_id'],
            'note_type': f'{specialty} Consultation',
            'note_date': encounter_data['encounter_date'] + timedelta(days=random.randint(0, 2)),
            'author': f"Dr. {self.fake.last_name()}, MD ({specialty})",
            'department': specialty,
            'note_content': note_content,
            'diagnosis_codes': [dx['diagnosis_code'] for dx in diagnosis_data],
            'created_date': encounter_data['encounter_date'],
            'updated_date': datetime.now()
        }
    
    def _calculate_age(self, birth_date) -> int:
        """Calculate age from birth date."""
        if isinstance(birth_date, str):
            birth_date = datetime.strptime(birth_date, '%Y-%m-%d').date()
        today = datetime.now().date()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    
    def _create_medical_header(self, patient_data: Dict, encounter_data: Dict) -> str:
        """Create a standard medical header for clinical notes."""
        patient_name = f"{patient_data['first_name']} {patient_data['last_name']}"
        
        # Format date of birth
        dob = patient_data['date_of_birth']
        if isinstance(dob, str):
            dob_formatted = datetime.strptime(dob, '%Y-%m-%d').strftime('%m/%d/%Y')
        else:
            dob_formatted = dob.strftime('%m/%d/%Y')
        
        # Format encounter date
        encounter_date = encounter_data['encounter_date']
        if isinstance(encounter_date, str):
            encounter_formatted = datetime.strptime(encounter_date, '%Y-%m-%d').strftime('%m/%d/%Y')
        else:
            encounter_formatted = encounter_date.strftime('%m/%d/%Y')
        
        return f"""PATIENT: {patient_name}
MRN: {patient_data.get('mrn', patient_data['patient_id'])}
DOB: {dob_formatted}
ENCOUNTER DATE: {encounter_formatted}
ATTENDING: {encounter_data['attending_physician']}
DEPARTMENT: {encounter_data['department']}

"""
    
    def _build_progress_note_content(self, patient_data: Dict, encounter_data: Dict, 
                                   diagnosis_data: List[Dict], age: int) -> str:
        """Build progress note content."""
        
        # Determine age group for appropriate language
        if age == 0:
            age_desc = "newborn"
        elif age <= 2:
            age_desc = f"{age}-year-old"
        elif age <= 12:
            age_desc = f"{age}-year-old child"
        else:
            age_desc = f"{age}-year-old adolescent"
        
        gender = "male" if patient_data['gender'] == 'M' else "female"
        
        # Chief complaint
        cc = encounter_data.get('chief_complaint', 'routine visit')
        
        # History of present illness
        symptoms = random.sample(self.pediatric_symptoms, random.randint(2, 4))
        
        # Add severity and occasional negation for realism
        severity_terms = ['mild', 'moderate', 'severe', 'intermittent', 'persistent', 'worsening', 'improving']
        def describe_symptom(sym: str) -> str:
            if random.random() < 0.2:
                return f"denies {sym}"
            if random.random() < 0.6:
                return f"{random.choice(severity_terms)} {sym}"
            return sym
        symptoms_desc = [describe_symptom(s) for s in symptoms]
        
        hpi = f"This {age_desc} {gender} presents with {cc}. "
        duration_days = random.randint(1, 10)
        if age <= 2:
            hpi += f"Parents report {', '.join(symptoms_desc[:-1])} and {symptoms_desc[-1]} for the past {duration_days} days. "
        else:
            hpi += f"Patient reports {', '.join(symptoms_desc[:-1])} and {symptoms_desc[-1]} for the past {duration_days} days. "
        
        # Tie to known diagnoses occasionally
        if diagnosis_data and random.random() < 0.6:
            dx_names = [dx.get('diagnosis_description', '') for dx in diagnosis_data[:3] if dx.get('diagnosis_description')]
            if dx_names:
                hpi += f"History notable for {', '.join(n.lower() for n in dx_names)}. "
        
        # Physical exam
        exam_sections = []
        for system, findings in self.physical_exam_findings.items():
            if random.random() < 0.8:  # Include most systems
                if system == 'vital_signs' and random.random() < 0.5:
                    finding = random.choice(['elevated temperature', 'tachycardic', 'tachypneic', 'normal for age'])
                else:
                    finding = random.choice(findings)
                exam_sections.append(f"{system.upper()}: {finding}")
        
        pe = "PHYSICAL EXAMINATION:\n" + "\n".join(exam_sections)
        
        # Assessment and plan
        if diagnosis_data:
            primary_dx = diagnosis_data[0]
            dx_code = primary_dx['diagnosis_code']
            
            if dx_code in self.assessment_plans:
                assessment = self.assessment_plans[dx_code]['assessment']
                plan_items = self.assessment_plans[dx_code]['plan']
            else:
                assessment = primary_dx['diagnosis_description']
                plan_items = [
                    'Continue current treatment',
                    'Monitor symptoms',
                    'Follow up as needed',
                    'Return if symptoms worsen'
                ]
            
            plan = "PLAN:\n" + "\n".join(f"- {item}" for item in plan_items)
        else:
            assessment = "Routine pediatric care"
            plan = "PLAN:\n- Continue routine care\n- Next appointment as scheduled"
        
        # Combine all sections with medical header
        header = self._create_medical_header(patient_data, encounter_data)
        note = f"""{header}CHIEF COMPLAINT: {cc.title()}

HISTORY OF PRESENT ILLNESS:
{hpi}

{pe}

ASSESSMENT: {assessment}

{plan}"""
        
        return note
    
    def _build_discharge_summary_content(self, patient_data: Dict, encounter_data: Dict,
                                       diagnosis_data: List[Dict], medications: List[Dict], age: int) -> str:
        """Build discharge summary content."""
        
        gender = "male" if patient_data['gender'] == 'M' else "female"
        age_desc = f"{age}-year-old" if age > 0 else "newborn"
        
        # Hospital course
        los = encounter_data.get('length_of_stay', 1)
        if los == 0:
            los = 1
            
        course = f"This {age_desc} {gender} was admitted for "
        if diagnosis_data:
            course += diagnosis_data[0]['diagnosis_description'].lower()
        else:
            course += "evaluation and treatment"
            
        course += f". During the {los}-day hospital stay, the patient "
        
        if age <= 2:
            course += "was monitored closely with supportive care. Parents were educated on care needs."
        else:
            course += "responded well to treatment and remained stable throughout the admission."
        
        # Discharge medications
        med_list = ""
        if medications:
            med_list = "DISCHARGE MEDICATIONS:\n"
            for med in medications[:5]:  # Limit to 5 medications
                med_list += f"- {med['medication_name']} {med['dosage']} {med['frequency']}\n"
        
        # Follow-up instructions
        followup = "FOLLOW-UP INSTRUCTIONS:\n"
        if diagnosis_data:
            dx_code = diagnosis_data[0]['diagnosis_code']
            if dx_code in self.assessment_plans:
                followup += "\n".join(f"- {item}" for item in self.assessment_plans[dx_code]['plan'])
            else:
                followup += "- Follow up with primary care provider in 1-2 weeks\n"
                followup += "- Return to ED if symptoms worsen"
        else:
            followup += "- Routine follow-up as previously scheduled"
        
        # Combine sections with medical header
        header = self._create_medical_header(patient_data, encounter_data)
        summary = f"""{header}DISCHARGE SUMMARY

ADMISSION DATE: {encounter_data['admission_date'].strftime('%m/%d/%Y')}
DISCHARGE DATE: {encounter_data.get('discharge_date', encounter_data['encounter_date']).strftime('%m/%d/%Y')}

FINAL DIAGNOSES:
{chr(10).join(f"- {dx['diagnosis_description']} ({dx['diagnosis_code']})" for dx in diagnosis_data)}

HOSPITAL COURSE:
{course}

{med_list}

{followup}

DISCHARGE CONDITION: Stable and improved"""
        
        return summary
    
    def _build_radiology_report_content(self, patient_data: Dict, encounter_data: Dict,
                                      study_type: str, age: int) -> str:
        """Build radiology report content."""
        
        age_desc = f"{age}-year-old" if age > 0 else "newborn"
        gender = "male" if patient_data['gender'] == 'M' else "female"
        
        # Study indication
        indication = encounter_data.get('chief_complaint', 'Clinical evaluation')
        
        # Technique
        techniques = {
            'chest_xray': 'Two-view chest radiograph (PA and lateral)',
            'abdominal_xray': 'Single-view abdominal radiograph (supine)',
            'brain_mri': 'Brain MRI with and without contrast'
        }
        
        technique = techniques.get(study_type, 'Standard imaging protocol')
        
        # Findings
        if study_type in self.radiology_findings:
            findings = random.sample(self.radiology_findings[study_type], random.randint(2, 4))
            findings_text = ". ".join(findings).capitalize() + "."
        else:
            findings_text = "No acute abnormalities identified."
        
        # Impression
        if random.random() < 0.9:  # 90% normal studies
            impression = "No acute abnormalities."
        else:
            impression = "Findings consistent with clinical presentation."
        
        report = f"""PATIENT: {patient_data['last_name']}, {patient_data['first_name']}
MRN: {patient_data['mrn']}
AGE: {age_desc} {gender}

STUDY: {study_type.replace('_', ' ').title()}
INDICATION: {indication}

TECHNIQUE: {technique}

FINDINGS: {findings_text}

IMPRESSION: {impression}

Electronically signed by:
Dr. {self.fake.last_name()}, MD
Department of Radiology
{encounter_data['encounter_date'].strftime('%m/%d/%Y %H:%M')}"""
        
        return report
    
    def _build_nursing_note_content(self, patient_data: Dict, encounter_data: Dict) -> str:
        """Build nursing note content."""
        
        observations = random.sample(self.nursing_observations, random.randint(3, 6))
        
        header = self._create_medical_header(patient_data, encounter_data)
        note = f"""{header}NURSING ASSESSMENT:

{chr(10).join(f"- {obs}" for obs in observations)}

Patient continues to be monitored per protocol. Family updated on plan of care.

{self.fake.first_name()} {self.fake.last_name()}, RN"""
        
        return note
    
    def _build_consultation_note_content(self, patient_data: Dict, encounter_data: Dict,
                                       specialty: str, diagnosis_data: List[Dict], age: int) -> str:
        """Build consultation note content."""
        
        age_desc = f"{age}-year-old" if age > 0 else "newborn"
        gender = "male" if patient_data['gender'] == 'M' else "female"
        
        # Consultation reason
        if diagnosis_data:
            reason = f"Consultation requested for {diagnosis_data[0]['diagnosis_description'].lower()}"
        else:
            reason = f"Consultation requested for evaluation"
        
        # Specialty-specific recommendations
        specialty_recs = {
            'Cardiology': [
                'Echo recommended to evaluate cardiac function',
                'Continue current cardiac medications',
                'Follow up in cardiology clinic in 3-6 months'
            ],
            'Neurology': [
                'EEG recommended if seizure activity suspected',
                'Continue current neurologic medications',
                'Developmental assessment recommended'
            ],
            'Pulmonology': [
                'Pulmonary function tests when age appropriate',
                'Continue bronchodilator therapy',
                'Asthma action plan reviewed'
            ]
        }
        
        recs = specialty_recs.get(specialty, [
            'Continue current management',
            'Follow up as clinically indicated',
            'Primary team to continue care'
        ])
        
        header = self._create_medical_header(patient_data, encounter_data)
        note = f"""{header}CONSULTATION NOTE - {specialty.upper()}

PATIENT: {age_desc} {gender}

REASON FOR CONSULTATION: {reason}

ASSESSMENT:
Thank you for this {specialty.lower()} consultation. I have reviewed the patient's history, examined the patient, and reviewed available studies.

RECOMMENDATIONS:
{chr(10).join(f"- {rec}" for rec in recs)}

I will continue to follow along with the primary team as needed.

Dr. {self.fake.last_name()}, MD
{specialty}"""
        
        return note


def main():
    """Generate sample clinical notes for testing."""
    generator = ClinicalNotesGenerator()
    
    # Sample data for testing
    sample_patient = {
        'patient_id': 'TCH-000001',
        'mrn': 'MRN12345678',
        'first_name': 'John',
        'last_name': 'Smith',
        'date_of_birth': datetime(2015, 5, 15).date(),
        'gender': 'M'
    }
    
    sample_encounter = {
        'encounter_id': 'ENC-00000001',
        'encounter_date': datetime.now(),
        'admission_date': datetime.now(),
        'discharge_date': datetime.now() + timedelta(days=1),
        'department': 'General Pediatrics',
        'attending_physician': 'Dr. Johnson, MD',
        'chief_complaint': 'fever and cough',
        'length_of_stay': 1
    }
    
    sample_diagnosis = [{
        'diagnosis_code': 'J06.9',
        'diagnosis_description': 'Upper respiratory infection'
    }]
    
    sample_medication = [{
        'medication_name': 'Acetaminophen',
        'dosage': '160 mg',
        'frequency': 'Every 6 hours as needed'
    }]
    
    # Generate various note types
    print("Generating sample clinical notes...")
    
    progress_note = generator.generate_progress_note(sample_patient, sample_encounter, sample_diagnosis)
    discharge_summary = generator.generate_discharge_summary(sample_patient, sample_encounter, sample_diagnosis, sample_medication)
    radiology_report = generator.generate_radiology_report(sample_patient, sample_encounter, 'chest_xray')
    nursing_note = generator.generate_nursing_note(sample_patient, sample_encounter)
    consultation_note = generator.generate_consultation_note(sample_patient, sample_encounter, 'Cardiology', sample_diagnosis)
    
    print("Sample Progress Note:")
    print("=" * 50)
    print(progress_note['note_content'])
    print("\n" + "=" * 50)
    
    print("\nSample Radiology Report:")
    print("=" * 50)
    print(radiology_report['note_content'])
    print("\n" + "=" * 50)


if __name__ == "__main__":
    main()