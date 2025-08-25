"""
Texas Children's Hospital Patient 360 PoC - Pediatric Data Generator

This module generates realistic synthetic healthcare data for a pediatric hospital
that mirrors the types of data Texas Children's Hospital would have from Epic EHR
and other source systems.
"""

import random
import uuid
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np
from faker import Faker
import json

class PediatricDataGenerator:
    """Generate realistic pediatric healthcare data for demonstration purposes."""
    
    def __init__(self, seed: int = 42):
        """Initialize generator with consistent seed for reproducible data."""
        random.seed(seed)
        np.random.seed(seed)
        self.fake = Faker()
        Faker.seed(seed)
        
        # Houston-area zip codes for realistic geographic distribution
        self.houston_zips = [
            '77001', '77002', '77003', '77004', '77005', '77006', '77007', '77008',
            '77009', '77010', '77011', '77012', '77013', '77014', '77015', '77016',
            '77017', '77018', '77019', '77020', '77021', '77022', '77023', '77024',
            '77025', '77026', '77027', '77028', '77029', '77030', '77031', '77032',
            '77033', '77034', '77035', '77036', '77037', '77038', '77039', '77040',
            '77041', '77042', '77043', '77044', '77045', '77046', '77047', '77048',
            '77049', '77050', '77051', '77052', '77053', '77054', '77055', '77056',
            '77057', '77058', '77059', '77060', '77061', '77062', '77063', '77064',
            '77065', '77066', '77067', '77068', '77069', '77070', '77071', '77072',
            '77073', '77074', '77075', '77076', '77077', '77078', '77079', '77080',
            '77081', '77082', '77083', '77084', '77085', '77086', '77087', '77088',
            '77089', '77090', '77091', '77092', '77093', '77094', '77095', '77096',
            '77097', '77098', '77099', '77338', '77339', '77345', '77346', '77347',
            '77354', '77357', '77365', '77373', '77375', '77377', '77379', '77380',
            '77381', '77382', '77383', '77384', '77385', '77386', '77388', '77389',
            '77391', '77393', '77396', '77401', '77402', '77406', '77407', '77429',
            '77433', '77447', '77449', '77450', '77459', '77469', '77477', '77478',
            '77479', '77484', '77489', '77493', '77494', '77498', '77502', '77503',
            '77504', '77505', '77506', '77507', '77508', '77520', '77521', '77530',
            '77532', '77536', '77539', '77546', '77547', '77562', '77571', '77573',
            '77581', '77584', '77586', '77587', '77598'
        ]
        
        # Common pediatric diagnoses with ICD-10 codes
        self.pediatric_diagnoses = {
            'J45.9': ('Asthma, unspecified', 0.08),  # 8% prevalence
            'F90.9': ('ADHD, unspecified', 0.07),   # 7% prevalence
            'E66.9': ('Obesity, unspecified', 0.18), # 18% prevalence
            'F84.0': ('Autistic disorder', 0.025),   # 2.5% prevalence
            'E10.9': ('Type 1 diabetes mellitus', 0.02),  # 2% prevalence
            'E11.9': ('Type 2 diabetes mellitus without complications', 0.005), # 0.5% prevalence (adolescents)
            'Q21.0': ('Ventricular septal defect', 0.008), # 0.8% prevalence
            'H52.13': ('Myopia', 0.12),             # 12% prevalence
            'L20.9': ('Atopic dermatitis', 0.15),   # 15% prevalence
            'K59.00': ('Constipation', 0.10),       # 10% prevalence
            'J06.9': ('Upper respiratory infection', 0.30), # 30% prevalence
            'B34.9': ('Viral infection', 0.25),     # 25% prevalence
            'K21.9': ('GERD', 0.08),                # 8% prevalence
            'G40.909': ('Epilepsy', 0.007),         # 0.7% prevalence
            'F32.9': ('Depression', 0.03),          # 3% prevalence (adolescents)
            'F41.9': ('Anxiety disorder', 0.05),    # 5% prevalence
            'M79.3': ('Growing pains', 0.15),       # 15% prevalence
            'Z00.129': ('Well child exam', 0.95),   # 95% have routine checkups
            'S72.001A': ('Fracture of femur', 0.02), # 2% prevalence
            'T78.40XA': ('Allergy, unspecified', 0.20), # 20% prevalence
            'H66.90': ('Otitis media', 0.35)        # 35% prevalence
        }
        
        # Department mappings for Texas Children's Hospital
        self.departments = [
            'Emergency Department', 'Pediatric ICU', 'NICU', 'Cardiology',
            'Neurology', 'Oncology', 'Orthopedics', 'Pulmonology',
            'Gastroenterology', 'Endocrinology', 'Nephrology', 'Rheumatology',
            'Dermatology', 'Ophthalmology', 'ENT', 'Psychiatry',
            'General Pediatrics', 'Adolescent Medicine', 'Newborn Nursery',
            'Ambulatory Surgery', 'Radiology', 'Laboratory', 'Pharmacy'
        ]
        
        # Common pediatric medications
        self.pediatric_medications = [
            'Acetaminophen', 'Ibuprofen', 'Amoxicillin', 'Azithromycin',
            'Albuterol', 'Fluticasone', 'Montelukast', 'Methylphenidate',
            'Insulin', 'Prednisone', 'Cetirizine', 'Diphenhydramine',
            'Omeprazole', 'Ranitidine', 'Simethicone', 'Polyethylene glycol',
            'Hydrocortisone', 'Mupirocin', 'Miconazole', 'Nystatin'
        ]
        
        # Lab test reference ranges by age
        self.lab_reference_ranges = {
            'Hemoglobin': {
                '0-1': (14.0, 20.0), '1-3': (9.5, 13.0), '4-6': (10.5, 13.5),
                '7-12': (11.0, 14.0), '13-15': (12.0, 15.2), '16-21': (12.6, 16.6)
            },
            'Hemoglobin A1c': {
                # Pediatric A1c general reference (normal < 5.7). We will allow generation up to ~13.5
                '0-21': (4.5, 5.7)
            },
            'White Blood Cells': {
                '0-1': (9000, 30000), '1-3': (6000, 17500), '4-6': (5500, 15500),
                '7-12': (4500, 13500), '13-21': (4500, 11000)
            },
            'Platelet Count': {
                '0-21': (150000, 450000)
            },
            'Glucose': {
                '0-21': (70, 100)
            },
            'Creatinine': {
                '0-1': (0.2, 0.4), '1-3': (0.3, 0.5), '4-6': (0.4, 0.6),
                '7-12': (0.5, 0.8), '13-21': (0.6, 1.2)
            }
        }
    
    def generate_patient_demographics(self, count: int) -> List[Dict]:
        """Generate realistic pediatric patient demographics."""
        patients = []
        
        for i in range(count):
            # Generate age with higher concentration in younger years
            age = self._generate_pediatric_age()
            birth_date = datetime.now() - timedelta(days=age * 365.25)
            
            # Generate basic demographics
            gender = random.choice(['M', 'F'])
            race = self._generate_race()
            ethnicity = self._generate_ethnicity()
            
            patient = {
                'patient_id': f"TCH-{i+1:06d}",
                'mrn': f"MRN{random.randint(10000000, 99999999)}",
                'first_name': self.fake.first_name_male() if gender == 'M' else self.fake.first_name_female(),
                'last_name': self.fake.last_name(),
                'date_of_birth': birth_date.date(),
                'age': age,
                'gender': gender,
                'race': race,
                'ethnicity': ethnicity,
                'zip_code': random.choice(self.houston_zips),
                'insurance_type': self._generate_insurance_type(age),
                'language': self._generate_language(ethnicity),
                'created_date': self.fake.date_time_between(start_date='-5y', end_date='now'),
                'updated_date': datetime.now()
            }
            patients.append(patient)
        
        return patients
    
    def generate_encounters(self, patients: List[Dict], encounters_per_patient: int = 5) -> List[Dict]:
        """Generate realistic encounter data for patients."""
        encounters = []
        encounter_id = 1
        
        for patient in patients:
            patient_age = patient['age']
            
            # Determine number of encounters based on age and conditions
            num_encounters = self._determine_encounter_count(patient_age, encounters_per_patient)
            
            for _ in range(num_encounters):
                encounter_date = self._generate_encounter_date(patient['created_date'])
                department = self._select_department(patient_age)
                encounter_type = self._determine_encounter_type(department)
                
                encounter = {
                    'encounter_id': f"ENC-{encounter_id:08d}",
                    'patient_id': patient['patient_id'],
                    'encounter_date': encounter_date,
                    'encounter_type': encounter_type,
                    'department': department,
                    'attending_physician': self._generate_physician_name(),
                    'admission_date': encounter_date,
                    'discharge_date': self._generate_discharge_date(encounter_date, encounter_type),
                    'length_of_stay': None,  # Will calculate based on dates
                    'chief_complaint': self._generate_chief_complaint(patient_age),
                    'status': random.choice(['Completed', 'In Progress', 'Scheduled']),
                    'created_date': encounter_date,
                    'updated_date': datetime.now()
                }
                
                # Calculate length of stay
                if encounter['discharge_date']:
                    encounter['length_of_stay'] = (encounter['discharge_date'] - encounter_date).days
                
                encounters.append(encounter)
                encounter_id += 1
        
        return encounters
    
    def generate_diagnoses(self, encounters: List[Dict]) -> List[Dict]:
        """Generate diagnosis data linked to encounters."""
        diagnoses = []
        diagnosis_id = 1
        
        for encounter in encounters:
            # Determine number of diagnoses for this encounter
            num_diagnoses = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
            
            selected_diagnoses = self._select_diagnoses_for_encounter(
                encounter['department'], num_diagnoses
            )
            
            for diag_code, diag_desc in selected_diagnoses:
                diagnosis = {
                    'diagnosis_id': f"DX-{diagnosis_id:08d}",
                    'encounter_id': encounter['encounter_id'],
                    'patient_id': encounter['patient_id'],
                    'diagnosis_code': diag_code,
                    'diagnosis_description': diag_desc,
                    'diagnosis_type': random.choice(['Primary', 'Secondary', 'Admitting']),
                    'diagnosis_date': encounter['encounter_date'],
                    'created_date': encounter['encounter_date'],
                    'updated_date': datetime.now()
                }
                diagnoses.append(diagnosis)
                diagnosis_id += 1
        
        return diagnoses
    
    def generate_lab_results(self, encounters: List[Dict], patients: List[Dict]) -> List[Dict]:
        """Generate realistic lab results."""
        lab_results = []
        lab_id = 1
        
        # Create patient lookup
        patient_lookup = {p['patient_id']: p for p in patients}
        
        # Filter encounters that would typically have lab work
        lab_encounters = [e for e in encounters if e['encounter_type'] in ['Inpatient', 'Emergency', 'Outpatient']]
        
        for encounter in lab_encounters:
            patient = patient_lookup[encounter['patient_id']]
            patient_age = patient['age']
            
            # Determine if labs are needed for this encounter
            if random.random() < 0.4:  # 40% of encounters have lab work
                lab_tests = self._select_lab_tests(encounter['department'])
                
                for test_name in lab_tests:
                    result_value, reference_range, abnormal_flag = self._generate_lab_value(
                        test_name, patient_age
                    )
                    
                    lab_result = {
                        'lab_result_id': f"LAB-{lab_id:08d}",
                        'encounter_id': encounter['encounter_id'],
                        'patient_id': encounter['patient_id'],
                        'test_name': test_name,
                        'test_value': result_value,
                        'reference_range': reference_range,
                        'abnormal_flag': abnormal_flag,
                        'result_date': encounter['encounter_date'] + timedelta(hours=random.randint(1, 24)),
                        'ordering_provider': encounter['attending_physician'],
                        'created_date': encounter['encounter_date'],
                        'updated_date': datetime.now()
                    }
                    lab_results.append(lab_result)
                    lab_id += 1
        
        return lab_results
    
    def generate_medications(self, encounters: List[Dict], diagnoses: List[Dict]) -> List[Dict]:
        """Generate medication data linked to encounters and diagnoses."""
        medications = []
        med_id = 1
        
        # Create diagnosis lookup
        encounter_diagnoses = {}
        for dx in diagnoses:
            if dx['encounter_id'] not in encounter_diagnoses:
                encounter_diagnoses[dx['encounter_id']] = []
            encounter_diagnoses[dx['encounter_id']].append(dx)
        
        for encounter in encounters:
            encounter_dx = encounter_diagnoses.get(encounter['encounter_id'], [])
            
            # Generate medications based on diagnoses
            medications_for_encounter = self._select_medications_for_diagnoses(encounter_dx)
            
            for med_name in medications_for_encounter:
                medication = {
                    'medication_id': f"MED-{med_id:08d}",
                    'encounter_id': encounter['encounter_id'],
                    'patient_id': encounter['patient_id'],
                    'medication_name': med_name,
                    'dosage': self._generate_dosage(med_name),
                    'frequency': random.choice(['Once daily', 'Twice daily', 'Three times daily', 'As needed']),
                    'route': random.choice(['Oral', 'IV', 'IM', 'Topical', 'Inhalation']),
                    'start_date': encounter['encounter_date'],
                    'end_date': encounter['encounter_date'] + timedelta(days=random.randint(1, 30)),
                    'prescribing_provider': encounter['attending_physician'],
                    'created_date': encounter['encounter_date'],
                    'updated_date': datetime.now()
                }
                medications.append(medication)
                med_id += 1
        
        return medications
    
    def generate_vital_signs(self, encounters: List[Dict], patients: List[Dict]) -> List[Dict]:
        """Generate vital signs data."""
        vital_signs = []
        vital_id = 1
        
        # Create patient lookup
        patient_lookup = {p['patient_id']: p for p in patients}
        
        for encounter in encounters:
            patient = patient_lookup[encounter['patient_id']]
            patient_age = patient['age']
            
            # Generate vital signs for most encounters
            if random.random() < 0.8:  # 80% of encounters have vitals
                vitals = self._generate_age_appropriate_vitals(patient_age)
                
                vital_sign = {
                    'vital_sign_id': f"VS-{vital_id:08d}",
                    'encounter_id': encounter['encounter_id'],
                    'patient_id': encounter['patient_id'],
                    'temperature': vitals['temperature'],
                    'heart_rate': vitals['heart_rate'],
                    'respiratory_rate': vitals['respiratory_rate'],
                    'blood_pressure_systolic': vitals['bp_systolic'],
                    'blood_pressure_diastolic': vitals['bp_diastolic'],
                    'oxygen_saturation': vitals['oxygen_sat'],
                    'weight_kg': vitals['weight'],
                    'height_cm': vitals['height'],
                    'recorded_date': encounter['encounter_date'] + timedelta(minutes=random.randint(15, 120)),
                    'recorded_by': f"Nurse {self.fake.last_name()}",
                    'created_date': encounter['encounter_date'],
                    'updated_date': datetime.now()
                }
                vital_signs.append(vital_sign)
                vital_id += 1
        
        return vital_signs
    
    def _generate_pediatric_age(self) -> int:
        """Generate age following realistic pediatric distribution."""
        # Higher concentration in younger ages
        age_weights = {
            0: 8, 1: 7, 2: 6, 3: 5, 4: 5, 5: 4, 6: 4, 7: 4, 8: 4, 9: 4,
            10: 3, 11: 3, 12: 3, 13: 3, 14: 3, 15: 3, 16: 3, 17: 3, 18: 2, 19: 2, 20: 2, 21: 2
        }
        ages = list(age_weights.keys())
        weights = list(age_weights.values())
        return random.choices(ages, weights=weights)[0]
    
    def _generate_race(self) -> str:
        """Generate race following Houston demographics."""
        races = ['White', 'Black or African American', 'Asian', 'American Indian', 'Pacific Islander', 'Other', 'Unknown']
        weights = [0.35, 0.22, 0.07, 0.01, 0.01, 0.25, 0.09]  # Houston area demographics
        return random.choices(races, weights=weights)[0]
    
    def _generate_ethnicity(self) -> str:
        """Generate ethnicity following Houston demographics."""
        ethnicities = ['Hispanic or Latino', 'Not Hispanic or Latino', 'Unknown']
        weights = [0.44, 0.51, 0.05]  # Houston area demographics
        return random.choices(ethnicities, weights=weights)[0]
    
    def _generate_insurance_type(self, age: int) -> str:
        """Generate insurance type based on age and demographics."""
        if age < 18:
            # Pediatric insurance distribution
            insurance_types = ['Medicaid', 'Commercial', 'CHIP', 'Self-pay', 'Other']
            weights = [0.45, 0.40, 0.10, 0.03, 0.02]
        else:
            # Young adult insurance distribution
            insurance_types = ['Commercial', 'Medicaid', 'Self-pay', 'Other']
            weights = [0.60, 0.25, 0.12, 0.03]
        
        return random.choices(insurance_types, weights=weights)[0]
    
    def _generate_language(self, ethnicity: str) -> str:
        """Generate primary language based on ethnicity."""
        if ethnicity == 'Hispanic or Latino':
            return random.choices(['Spanish', 'English'], weights=[0.7, 0.3])[0]
        else:
            return random.choices(['English', 'Spanish', 'Other'], weights=[0.85, 0.10, 0.05])[0]
    
    def _determine_encounter_count(self, age: int, base_count: int) -> int:
        """Determine number of encounters based on age."""
        if age == 0:
            # Newborns have more encounters
            return random.randint(base_count + 3, base_count + 8)
        elif age <= 2:
            # Toddlers have frequent checkups
            return random.randint(base_count + 1, base_count + 4)
        elif age <= 5:
            # Preschoolers
            return random.randint(base_count, base_count + 3)
        else:
            # School age and adolescents
            return random.randint(max(1, base_count - 2), base_count + 2)
    
    def _generate_encounter_date(self, created_date: datetime) -> datetime:
        """Generate realistic encounter date."""
        # Encounters should be after patient creation
        start_date = max(created_date, datetime.now() - timedelta(days=365*3))
        end_date = datetime.now()
        return self.fake.date_time_between(start_date=start_date, end_date=end_date)
    
    def _select_department(self, age: int) -> str:
        """Select appropriate department based on age."""
        if age == 0:
            # Newborns
            return random.choices(
                ['NICU', 'Newborn Nursery', 'Pediatric ICU', 'Emergency Department'],
                weights=[0.15, 0.70, 0.05, 0.10]
            )[0]
        elif age <= 2:
            # Toddlers
            return random.choices(
                ['General Pediatrics', 'Emergency Department', 'Pediatric ICU'],
                weights=[0.80, 0.15, 0.05]
            )[0]
        else:
            # Older children
            return random.choices(
                self.departments,
                weights=[0.12, 0.03, 0.01, 0.08, 0.06, 0.02, 0.05, 0.04, 0.04, 0.03, 0.02, 0.02, 
                        0.03, 0.02, 0.02, 0.03, 0.35, 0.04, 0.01, 0.02, 0.01, 0.01, 0.01]
            )[0]
    
    def _determine_encounter_type(self, department: str) -> str:
        """Determine encounter type based on department."""
        if department in ['Emergency Department']:
            return 'Emergency'
        elif department in ['Pediatric ICU', 'NICU']:
            return 'Inpatient'
        elif department in ['Ambulatory Surgery', 'Radiology']:
            return 'Outpatient'
        else:
            return random.choices(['Outpatient', 'Inpatient'], weights=[0.85, 0.15])[0]
    
    def _generate_discharge_date(self, admission_date: datetime, encounter_type: str) -> Optional[datetime]:
        """Generate discharge date based on encounter type."""
        if encounter_type == 'Outpatient':
            return admission_date  # Same day discharge
        elif encounter_type == 'Emergency':
            # Emergency visits can be same day or short stay
            if random.random() < 0.8:
                return admission_date
            else:
                return admission_date + timedelta(days=random.randint(1, 3))
        else:  # Inpatient
            los = random.choices([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30], 
                               weights=[0.2, 0.2, 0.15, 0.12, 0.1, 0.08, 0.05, 0.03, 0.02, 0.02, 0.01, 0.01, 0.01])[0]
            return admission_date + timedelta(days=los)
    
    def _generate_physician_name(self) -> str:
        """Generate realistic physician name."""
        return f"Dr. {self.fake.first_name()} {self.fake.last_name()}, MD"
    
    def _generate_chief_complaint(self, age: int) -> str:
        """Generate age-appropriate chief complaints."""
        if age == 0:
            complaints = ['Feeding difficulties', 'Respiratory distress', 'Fever', 'Jaundice', 'Poor weight gain']
        elif age <= 2:
            complaints = ['Fever', 'Cough', 'Vomiting', 'Diarrhea', 'Rash', 'Irritability', 'Poor feeding']
        elif age <= 12:
            complaints = ['Fever', 'Cough', 'Abdominal pain', 'Headache', 'Sore throat', 'Ear pain', 'Rash']
        else:
            complaints = ['Headache', 'Abdominal pain', 'Chest pain', 'Anxiety', 'Depression', 'Sports injury', 'Acne']
        
        return random.choice(complaints)
    
    def _select_diagnoses_for_encounter(self, department: str, count: int) -> List[Tuple[str, str]]:
        """Select appropriate diagnoses for encounter."""
        # Filter diagnoses by department appropriateness
        if department == 'Emergency Department':
            common_codes = ['J06.9', 'B34.9', 'S72.001A', 'T78.40XA']
        elif department == 'Cardiology':
            common_codes = ['Q21.0']
        elif department == 'Pulmonology':
            common_codes = ['J45.9']
        elif department == 'Endocrinology':
            common_codes = ['E10.9', 'E66.9']
        else:
            common_codes = list(self.pediatric_diagnoses.keys())
        
        # Select diagnoses based on prevalence
        selected = []
        for _ in range(count):
            code = random.choice(common_codes)
            desc = self.pediatric_diagnoses[code][0]
            selected.append((code, desc))
        
        return selected
    
    def _select_lab_tests(self, department: str) -> List[str]:
        """Select appropriate lab tests for department."""
        common_tests = ['Hemoglobin', 'White Blood Cells', 'Platelet Count']
        
        if department in ['Emergency Department', 'Pediatric ICU']:
            tests = common_tests + ['Glucose', 'Creatinine']
        elif department == 'Endocrinology':
            tests = ['Hemoglobin A1c', 'Glucose', 'Thyroid Function']
        else:
            tests = common_tests
        
        # Return random subset but ensure HbA1c is included when present
        num_tests = random.randint(1, len(tests))
        selected = set(random.sample(tests, num_tests))
        if 'Hemoglobin A1c' in tests:
            selected.add('Hemoglobin A1c')
        return list(selected)
    
    def _generate_lab_value(self, test_name: str, age: int) -> Tuple[str, str, str]:
        """Generate realistic lab value based on age."""
        # Special handling for HbA1c to ensure numeric values and a realistic tail > 9 for some cases
        if test_name == 'Hemoglobin A1c':
            # 75% normal (4.8-6.0), 15% elevated (6.5-8.9), 10% very high (9.0-13.5)
            roll = random.random()
            if roll < 0.75:
                value = round(random.uniform(4.8, 6.0), 1)
                abnormal_flag = ""
            elif roll < 0.90:
                value = round(random.uniform(6.5, 8.9), 1)
                abnormal_flag = "H"
            else:
                value = round(random.uniform(9.0, 13.5), 1)
                abnormal_flag = "H"
            ref_min, ref_max = self.lab_reference_ranges['Hemoglobin A1c']['0-21']
            reference_range = f"{ref_min}-{ref_max}"
            return f"{value:.1f}", reference_range, abnormal_flag

        if test_name not in self.lab_reference_ranges:
            return "Normal", "Reference range not defined", ""
        
        ranges = self.lab_reference_ranges[test_name]
        
        # Find appropriate age range
        age_range = None
        for range_key in ranges.keys():
            if '-' in range_key:
                min_age, max_age = map(int, range_key.split('-'))
                if min_age <= age <= max_age:
                    age_range = range_key
                    break
            else:
                age_range = range_key
                break
        
        if age_range:
            min_val, max_val = ranges[age_range]
            
            # 90% normal values, 10% abnormal
            if random.random() < 0.9:
                value = random.uniform(min_val, max_val)
                abnormal_flag = ""
            else:
                # Generate abnormal value
                if random.random() < 0.5:
                    value = random.uniform(min_val * 0.7, min_val)
                    abnormal_flag = "L"
                else:
                    value = random.uniform(max_val, max_val * 1.3)
                    abnormal_flag = "H"
            
            if test_name in ['White Blood Cells', 'Platelet Count']:
                value_str = str(int(value))
            else:
                value_str = f"{value:.1f}"
            
            reference_range = f"{min_val}-{max_val}"
            
            return value_str, reference_range, abnormal_flag
        
        return "Normal", "Reference range not available", ""
    
    def _select_medications_for_diagnoses(self, diagnoses: List[Dict]) -> List[str]:
        """Select appropriate medications based on diagnoses."""
        medications = []
        
        for dx in diagnoses:
            code = dx['diagnosis_code']
            
            if code == 'J45.9':  # Asthma
                medications.extend(['Albuterol', 'Fluticasone'])
            elif code == 'F90.9':  # ADHD
                medications.append('Methylphenidate')
            elif code == 'E10.9':  # Diabetes
                medications.append('Insulin')
            elif code.startswith('J06') or code.startswith('B34'):  # Infections
                medications.extend(['Acetaminophen', 'Ibuprofen'])
            elif code == 'K21.9':  # GERD
                medications.append('Omeprazole')
            else:
                # General supportive care
                medications.extend(['Acetaminophen', 'Ibuprofen'])
        
        # Remove duplicates and return subset
        unique_meds = list(set(medications))
        if len(unique_meds) > 3:
            return random.sample(unique_meds, 3)
        return unique_meds
    
    def _generate_dosage(self, medication: str) -> str:
        """Generate realistic dosage for medication."""
        dosages = {
            'Acetaminophen': ['10-15 mg/kg/dose', '80 mg', '160 mg', '325 mg'],
            'Ibuprofen': ['5-10 mg/kg/dose', '50 mg', '100 mg', '200 mg'],
            'Albuterol': ['2 puffs', '0.083% nebulizer solution'],
            'Methylphenidate': ['5 mg', '10 mg', '18 mg', '27 mg'],
            'Insulin': ['Per sliding scale', 'Units as directed'],
            'Omeprazole': ['10 mg', '20 mg', '40 mg']
        }
        
        if medication in dosages:
            return random.choice(dosages[medication])
        else:
            return "As directed"
    
    def _generate_age_appropriate_vitals(self, age: int) -> Dict:
        """Generate age-appropriate vital signs."""
        if age == 0:  # Newborn
            return {
                'temperature': round(random.uniform(36.5, 37.2), 1),
                'heart_rate': random.randint(120, 160),
                'respiratory_rate': random.randint(30, 60),
                'bp_systolic': random.randint(65, 95),
                'bp_diastolic': random.randint(30, 60),
                'oxygen_sat': random.randint(95, 100),
                'weight': round(random.uniform(2.5, 4.5), 2),
                'height': round(random.uniform(45, 55), 1)
            }
        elif age <= 1:  # Infant
            return {
                'temperature': round(random.uniform(36.5, 37.2), 1),
                'heart_rate': random.randint(100, 150),
                'respiratory_rate': random.randint(25, 50),
                'bp_systolic': random.randint(70, 100),
                'bp_diastolic': random.randint(35, 65),
                'oxygen_sat': random.randint(95, 100),
                'weight': round(random.uniform(4, 12), 2),
                'height': round(random.uniform(50, 80), 1)
            }
        elif age <= 12:  # Child
            return {
                'temperature': round(random.uniform(36.5, 37.2), 1),
                'heart_rate': random.randint(80, 120),
                'respiratory_rate': random.randint(15, 25),
                'bp_systolic': random.randint(90, 110),
                'bp_diastolic': random.randint(55, 70),
                'oxygen_sat': random.randint(95, 100),
                'weight': round(random.uniform(12, 50), 2),
                'height': round(random.uniform(75, 150), 1)
            }
        else:  # Adolescent
            return {
                'temperature': round(random.uniform(36.5, 37.2), 1),
                'heart_rate': random.randint(60, 100),
                'respiratory_rate': random.randint(12, 20),
                'bp_systolic': random.randint(100, 120),
                'bp_diastolic': random.randint(60, 80),
                'oxygen_sat': random.randint(95, 100),
                'weight': round(random.uniform(40, 80), 2),
                'height': round(random.uniform(140, 180), 1)
            }


def main():
    """Generate sample data for testing."""
    generator = PediatricDataGenerator()
    
    # Generate small sample for testing
    print("Generating sample pediatric healthcare data...")
    
    patients = generator.generate_patient_demographics(100)
    encounters = generator.generate_encounters(patients, 3)
    diagnoses = generator.generate_diagnoses(encounters)
    lab_results = generator.generate_lab_results(encounters, patients)
    medications = generator.generate_medications(encounters, diagnoses)
    vital_signs = generator.generate_vital_signs(encounters, patients)
    
    print(f"Generated:")
    print(f"  - {len(patients)} patients")
    print(f"  - {len(encounters)} encounters")
    print(f"  - {len(diagnoses)} diagnoses")
    print(f"  - {len(lab_results)} lab results")
    print(f"  - {len(medications)} medications")
    print(f"  - {len(vital_signs)} vital signs")
    
    # Sample outputs
    print("\nSample patient:")
    print(json.dumps(patients[0], indent=2, default=str))
    
    print("\nSample encounter:")
    print(json.dumps(encounters[0], indent=2, default=str))


if __name__ == "__main__":
    main()