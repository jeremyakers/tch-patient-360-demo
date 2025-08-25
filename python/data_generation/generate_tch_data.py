#!/usr/bin/env python3
"""
Texas Children's Hospital Patient 360 PoC - Master Data Generation Script

This script generates comprehensive, realistic synthetic healthcare data for the
Texas Children's Hospital Patient 360 PoC demonstration, including both structured
and unstructured data sources.
"""

import os
import sys
import argparse
import json
import csv
import gzip
import shutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
from pathlib import Path

# Add the project root to the path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generation.pediatric_data_generator import PediatricDataGenerator
from data_generation.clinical_notes_generator import ClinicalNotesGenerator

class TCHDataGenerationOrchestrator:
    """Orchestrates the generation of all TCH PoC data."""
    
    def __init__(self, output_dir: str = "data/mock_data", seed: int = 42, compress_files: bool = False):
        """Initialize the data generation orchestrator."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.compress_files = compress_files
        
        # Create subdirectories for different data types
        self.structured_dir = self.output_dir / "structured"
        self.unstructured_dir = self.output_dir / "unstructured"
        self.structured_dir.mkdir(exist_ok=True)
        self.unstructured_dir.mkdir(exist_ok=True)
        
        # Clean up old files before generating new ones
        self._cleanup_old_files()
        
        # Initialize generators
        self.pediatric_generator = PediatricDataGenerator(seed=seed)
        self.notes_generator = ClinicalNotesGenerator(seed=seed)
        
        compression_note = " (with gzip compression)" if compress_files else ""
        print(f"Data generation output directory: {self.output_dir.absolute()}{compression_note}")
    
    def _cleanup_old_files(self):
        """Clean up old data files before generating new ones."""
        print("🧹 Cleaning up old data files...")
        
        # Clean structured data files
        structured_patterns = ['*.csv', '*.csv.gz']
        for pattern in structured_patterns:
            for old_file in self.structured_dir.glob(pattern):
                old_file.unlink()
                print(f"   Removed: {old_file.name}")
        
        # Clean unstructured data files - remove subdirectories and files
        if self.unstructured_dir.exists():
            for item in self.unstructured_dir.iterdir():
                if item.is_dir():
                    shutil.rmtree(item)
                    print(f"   Removed directory: {item.name}")
                else:
                    item.unlink()
                    print(f"   Removed file: {item.name}")
        
        # Clean any metadata files
        for metadata_file in self.output_dir.glob('*.json'):
            metadata_file.unlink()
            print(f"   Removed metadata: {metadata_file.name}")
        
        print("✅ Cleanup completed")
    
    def generate_complete_dataset(self, num_patients: int = 500000, 
                                encounters_per_patient: int = 5) -> Dict[str, int]:
        """Generate complete dataset for TCH PoC."""
        
        print(f"Starting generation of comprehensive TCH dataset...")
        print(f"Target: {num_patients:,} patients with ~{encounters_per_patient} encounters each")
        print(f"Expected total encounters: ~{num_patients * encounters_per_patient:,}")
        
        stats = {}
        
        # Generate core structured data
        print("\n1. Generating patient demographics...")
        patients = self.pediatric_generator.generate_patient_demographics(num_patients)
        self._save_to_csv(patients, 'patients.csv')
        stats['patients'] = len(patients)
        print(f"   Generated {len(patients):,} patients")
        
        print("\n2. Generating encounters...")
        encounters = self.pediatric_generator.generate_encounters(patients, encounters_per_patient)
        self._save_to_csv(encounters, 'encounters.csv')
        stats['encounters'] = len(encounters)
        print(f"   Generated {len(encounters):,} encounters")
        
        print("\n3. Generating diagnoses...")
        diagnoses = self.pediatric_generator.generate_diagnoses(encounters)
        self._save_to_csv(diagnoses, 'diagnoses.csv')
        stats['diagnoses'] = len(diagnoses)
        print(f"   Generated {len(diagnoses):,} diagnoses")
        
        print("\n4. Generating lab results...")
        lab_results = self.pediatric_generator.generate_lab_results(encounters, patients)
        self._save_to_csv(lab_results, 'lab_results.csv')
        stats['lab_results'] = len(lab_results)
        print(f"   Generated {len(lab_results):,} lab results")
        
        print("\n5. Generating medications...")
        medications = self.pediatric_generator.generate_medications(encounters, diagnoses)
        self._save_to_csv(medications, 'medications.csv')
        stats['medications'] = len(medications)
        print(f"   Generated {len(medications):,} medications")
        
        print("\n6. Generating vital signs...")
        vital_signs = self.pediatric_generator.generate_vital_signs(encounters, patients)
        self._save_to_csv(vital_signs, 'vital_signs.csv')
        stats['vital_signs'] = len(vital_signs)
        print(f"   Generated {len(vital_signs):,} vital signs records")
        
        # Generate additional healthcare data
        print("\n7. Generating imaging studies...")
        imaging_studies = self._generate_imaging_studies(encounters, patients)
        self._save_to_csv(imaging_studies, 'imaging_studies.csv')
        stats['imaging_studies'] = len(imaging_studies)
        print(f"   Generated {len(imaging_studies):,} imaging studies")
        
        print("\n8. Generating provider data...")
        providers = self._generate_providers()
        self._save_to_csv(providers, 'providers.csv')
        stats['providers'] = len(providers)
        print(f"   Generated {len(providers):,} providers")
        
        print("\n9. Generating department data...")
        departments = self._generate_departments()
        self._save_to_csv(departments, 'departments.csv')
        stats['departments'] = len(departments)
        print(f"   Generated {len(departments):,} departments")
        
        # Generate unstructured clinical documentation
        print("\n10. Generating clinical notes...")
        clinical_notes = self._generate_clinical_notes(encounters, patients, diagnoses, medications)
        stats['clinical_notes'] = len(clinical_notes)
        print(f"    Generated {len(clinical_notes):,} clinical notes")
        
        print("\n11. Generating radiology reports...")
        radiology_reports = self._generate_radiology_reports(imaging_studies, patients, encounters)
        stats['radiology_reports'] = len(radiology_reports)
        print(f"    Generated {len(radiology_reports):,} radiology reports")
        
        # Generate summary statistics and metadata
        print("\n12. Generating metadata and statistics...")
        self._generate_metadata(stats, num_patients, encounters_per_patient)
        
        print(f"\n✅ Data generation complete!")
        print(f"\nFinal statistics:")
        for category, count in stats.items():
            print(f"   {category}: {count:,}")
        
        # Count files (both compressed and uncompressed)
        csv_files = list(self.structured_dir.glob("*.csv")) + list(self.structured_dir.glob("*.csv.gz"))
        txt_files = list(self.unstructured_dir.glob("*.txt")) + list(self.unstructured_dir.glob("*.txt.gz"))
        total_files = len(csv_files) + len(txt_files)
        print(f"\n📁 Generated {total_files} data files")
        print(f"   Structured data: {self.structured_dir}")
        print(f"   Unstructured data: {self.unstructured_dir}")
        
        return stats
    
    def _save_to_csv(self, data: List[Dict], filename: str):
        """Save data to CSV file with proper handling of datetime objects."""
        if not data:
            print(f"   Warning: No data to save for {filename}")
            return
        
        # Add .gz extension if compression is enabled
        if self.compress_files:
            if not filename.endswith('.gz'):
                filename = filename + '.gz'
        
        filepath = self.structured_dir / filename
        
        # Convert data to DataFrame for better CSV handling
        df = pd.DataFrame(data)
        
        # Convert datetime columns to strings
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check if column contains datetime objects
                sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample_val, (datetime, pd.Timestamp)):
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                elif hasattr(sample_val, 'strftime'):  # date objects
                    df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
        
        # Save with or without compression
        if self.compress_files:
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                df.to_csv(f, index=False, quoting=csv.QUOTE_NONNUMERIC)
        else:
            df.to_csv(filepath, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        file_size = filepath.stat().st_size / (1024 * 1024)  # Size in MB
        compression_note = " (compressed)" if self.compress_files else ""
        print(f"   Saved {len(data):,} records to {filename} ({file_size:.1f} MB){compression_note}")
    
    def _save_text_file(self, content: str, filename: str, subdir: str = ""):
        """Save text content to file."""
        if subdir:
            target_dir = self.unstructured_dir / subdir
            target_dir.mkdir(exist_ok=True)
        else:
            target_dir = self.unstructured_dir
        
        # Add .gz extension if compression is enabled
        if self.compress_files:
            if not filename.endswith('.gz'):
                filename = filename + '.gz'
        
        filepath = target_dir / filename
        
        # Save with or without compression
        if self.compress_files:
            with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                f.write(content)
        else:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
    
    def _generate_imaging_studies(self, encounters: List[Dict], patients: List[Dict]) -> List[Dict]:
        """Generate imaging study records."""
        imaging_studies = []
        study_id = 1
        
        # Create patient lookup
        patient_lookup = {p['patient_id']: p for p in patients}
        
        # Common pediatric imaging studies
        study_types = [
            ('chest_xray', 'Chest X-ray', 0.15),
            ('abdominal_xray', 'Abdominal X-ray', 0.08),
            ('brain_mri', 'Brain MRI', 0.03),
            ('brain_ct', 'Brain CT', 0.05),
            ('ultrasound_abdomen', 'Abdominal Ultrasound', 0.06),
            ('echo', 'Echocardiogram', 0.04)
        ]
        
        for encounter in encounters:
            patient = patient_lookup[encounter['patient_id']]
            
            # Determine if imaging is needed based on encounter type and department
            imaging_probability = 0.1  # Base 10% chance
            
            if encounter['encounter_type'] == 'Emergency':
                imaging_probability = 0.25
            elif encounter['department'] in ['Pediatric ICU', 'NICU']:
                imaging_probability = 0.4
            elif encounter['department'] in ['Cardiology', 'Pulmonology', 'Neurology']:
                imaging_probability = 0.3
            
            if random.random() < imaging_probability:
                # Select appropriate study type
                study_code, study_name, _ = random.choice(study_types)
                
                imaging_study = {
                    'imaging_study_id': f"IMG-{study_id:08d}",
                    'encounter_id': encounter['encounter_id'],
                    'patient_id': encounter['patient_id'],
                    'study_type': study_code,
                    'study_name': study_name,
                    'study_date': encounter['encounter_date'] + timedelta(hours=random.randint(1, 24)),
                    'ordering_provider': encounter['attending_physician'],
                    'performing_department': 'Radiology',
                    'study_status': random.choice(['Completed', 'Preliminary', 'Final']),
                    'modality': self._get_modality_for_study(study_code),
                    'body_part': self._get_body_part_for_study(study_code),
                    'created_date': encounter['encounter_date'],
                    'updated_date': datetime.now()
                }
                imaging_studies.append(imaging_study)
                study_id += 1
        
        return imaging_studies
    
    def _get_modality_for_study(self, study_type: str) -> str:
        """Get imaging modality for study type."""
        modality_map = {
            'chest_xray': 'XR',
            'abdominal_xray': 'XR',
            'brain_mri': 'MR',
            'brain_ct': 'CT',
            'ultrasound_abdomen': 'US',
            'echo': 'US'
        }
        return modality_map.get(study_type, 'XR')
    
    def _get_body_part_for_study(self, study_type: str) -> str:
        """Get body part for study type."""
        body_part_map = {
            'chest_xray': 'Chest',
            'abdominal_xray': 'Abdomen',
            'brain_mri': 'Brain',
            'brain_ct': 'Brain',
            'ultrasound_abdomen': 'Abdomen',
            'echo': 'Heart'
        }
        return body_part_map.get(study_type, 'Chest')
    
    def _generate_providers(self) -> List[Dict]:
        """Generate provider/physician data."""
        providers = []
        
        # Specialties at TCH
        specialties = [
            'General Pediatrics', 'Emergency Medicine', 'Pediatric Critical Care',
            'Neonatology', 'Cardiology', 'Neurology', 'Oncology', 'Orthopedics',
            'Pulmonology', 'Gastroenterology', 'Endocrinology', 'Nephrology',
            'Rheumatology', 'Dermatology', 'Ophthalmology', 'ENT', 'Psychiatry',
            'Adolescent Medicine', 'Radiology', 'Pathology', 'Anesthesiology'
        ]
        
        provider_id = 1
        for specialty in specialties:
            # Generate 10-20 providers per specialty
            num_providers = random.randint(10, 20)
            for _ in range(num_providers):
                provider = {
                    'provider_id': f"PROV-{provider_id:06d}",
                    'npi': f"{random.randint(1000000000, 9999999999)}",
                    'first_name': self.pediatric_generator.fake.first_name(),
                    'last_name': self.pediatric_generator.fake.last_name(),
                    'specialty': specialty,
                    'department': specialty,
                    'credentials': random.choice(['MD', 'DO', 'MD, PhD']),
                    'status': random.choice(['Active', 'Active', 'Active', 'Inactive']),  # Mostly active
                    'hire_date': self.pediatric_generator.fake.date_between(start_date='-20y', end_date='-1y'),
                    'created_date': datetime.now() - timedelta(days=random.randint(30, 1000)),
                    'updated_date': datetime.now()
                }
                providers.append(provider)
                provider_id += 1
        
        return providers
    
    def _generate_departments(self) -> List[Dict]:
        """Generate department/service line data."""
        departments = []
        
        dept_info = [
            ('Emergency Department', 'ED', 'Emergency Medicine'),
            ('Pediatric ICU', 'PICU', 'Critical Care'),
            ('NICU', 'NICU', 'Neonatology'),
            ('General Pediatrics', 'PEDS', 'Ambulatory'),
            ('Cardiology', 'CARDS', 'Specialty'),
            ('Neurology', 'NEURO', 'Specialty'),
            ('Oncology', 'ONCO', 'Specialty'),
            ('Orthopedics', 'ORTHO', 'Specialty'),
            ('Pulmonology', 'PULM', 'Specialty'),
            ('Gastroenterology', 'GI', 'Specialty'),
            ('Endocrinology', 'ENDO', 'Specialty'),
            ('Nephrology', 'NEPHRO', 'Specialty'),
            ('Radiology', 'RAD', 'Ancillary'),
            ('Laboratory', 'LAB', 'Ancillary'),
            ('Pharmacy', 'PHARM', 'Ancillary')
        ]
        
        for i, (name, code, service_line) in enumerate(dept_info, 1):
            department = {
                'department_id': f"DEPT-{i:03d}",
                'department_name': name,
                'department_code': code,
                'service_line': service_line,
                'location': random.choice(['Main Campus', 'West Campus', 'The Woodlands']),
                'status': 'Active',
                'created_date': datetime.now() - timedelta(days=random.randint(100, 2000)),
                'updated_date': datetime.now()
            }
            departments.append(department)
        
        return departments
    
    def _generate_clinical_notes(self, encounters: List[Dict], patients: List[Dict], 
                               diagnoses: List[Dict], medications: List[Dict]) -> List[Dict]:
        """Generate clinical notes and documentation."""
        clinical_notes = []
        
        # Create lookups for efficient access
        patient_lookup = {p['patient_id']: p for p in patients}
        encounter_diagnoses = {}
        encounter_medications = {}
        
        for dx in diagnoses:
            if dx['encounter_id'] not in encounter_diagnoses:
                encounter_diagnoses[dx['encounter_id']] = []
            encounter_diagnoses[dx['encounter_id']].append(dx)
        
        for med in medications:
            if med['encounter_id'] not in encounter_medications:
                encounter_medications[med['encounter_id']] = []
            encounter_medications[med['encounter_id']].append(med)
        
        # Generate notes for subset of encounters (performance consideration)
        sample_encounters = random.sample(encounters, min(len(encounters), 100000))
        
        for i, encounter in enumerate(sample_encounters):
            if i % 10000 == 0:
                print(f"    Generating notes for encounter {i+1:,} of {len(sample_encounters):,}")
            
            patient = patient_lookup[encounter['patient_id']]
            enc_diagnoses = encounter_diagnoses.get(encounter['encounter_id'], [])
            enc_medications = encounter_medications.get(encounter['encounter_id'], [])
            
            # Generate different types of notes based on encounter
            note_types_to_generate = ['progress']
            
            if encounter['encounter_type'] == 'Inpatient':
                note_types_to_generate.extend(['nursing', 'discharge'])
            
            if encounter['encounter_type'] == 'Emergency':
                note_types_to_generate.append('nursing')
            
            if encounter['department'] in ['Cardiology', 'Neurology', 'Pulmonology']:
                note_types_to_generate.append('consultation')
            
            # Generate each type of note
            for note_type in note_types_to_generate:
                try:
                    if note_type == 'progress':
                        note = self.notes_generator.generate_progress_note(patient, encounter, enc_diagnoses)
                    elif note_type == 'nursing':
                        note = self.notes_generator.generate_nursing_note(patient, encounter)
                    elif note_type == 'discharge':
                        note = self.notes_generator.generate_discharge_summary(patient, encounter, enc_diagnoses, enc_medications)
                    elif note_type == 'consultation':
                        note = self.notes_generator.generate_consultation_note(patient, encounter, encounter['department'], enc_diagnoses)
                    
                    clinical_notes.append(note)
                    
                    # Save note content to individual text file
                    filename = f"note_{note['note_id']}.txt"
                    self._save_text_file(note['note_content'], filename, "clinical_notes")
                    
                except Exception as e:
                    print(f"    Error generating {note_type} note for encounter {encounter['encounter_id']}: {e}")
                    continue
        
        # Save clinical notes metadata
        self._save_to_csv(clinical_notes, 'clinical_notes.csv')
        
        return clinical_notes
    
    def _generate_radiology_reports(self, imaging_studies: List[Dict], patients: List[Dict], 
                                  encounters: List[Dict]) -> List[Dict]:
        """Generate radiology reports."""
        radiology_reports = []
        
        # Create lookups
        patient_lookup = {p['patient_id']: p for p in patients}
        encounter_lookup = {e['encounter_id']: e for e in encounters}
        
        for study in imaging_studies:
            if study['study_status'] in ['Completed', 'Final']:
                try:
                    patient = patient_lookup[study['patient_id']]
                    encounter = encounter_lookup[study['encounter_id']]
                    
                    report = self.notes_generator.generate_radiology_report(
                        patient, encounter, study['study_type']
                    )
                    
                    # Add study-specific information
                    report['imaging_study_id'] = study['imaging_study_id']
                    report['study_type'] = study['study_type']
                    
                    radiology_reports.append(report)
                    
                    # Save report content to text file
                    filename = f"radiology_{report['note_id']}.txt"
                    self._save_text_file(report['note_content'], filename, "radiology_reports")
                    
                except Exception as e:
                    print(f"    Error generating radiology report for study {study['imaging_study_id']}: {e}")
                    continue
        
        # Save radiology reports metadata
        self._save_to_csv(radiology_reports, 'radiology_reports.csv')
        
        return radiology_reports
    
    def _generate_metadata(self, stats: Dict[str, int], num_patients: int, encounters_per_patient: int):
        """Generate metadata about the dataset."""
        metadata = {
            'generated_date': datetime.now().isoformat(),
            'generator_version': '1.0.0',
            'target_patients': num_patients,
            'target_encounters_per_patient': encounters_per_patient,
            'actual_statistics': stats,
            'data_characteristics': {
                'age_distribution': 'Pediatric (0-21 years) with higher concentration in younger ages',
                'geographic_focus': 'Houston metropolitan area zip codes',
                'insurance_mix': 'Realistic pediatric insurance distribution (Medicaid, Commercial, CHIP)',
                'condition_prevalence': 'Matches real-world pediatric disease prevalence rates',
                'clinical_realism': 'Age-appropriate diagnoses, medications, and vital signs'
            },
            'file_structure': {
                'structured_data': str(self.structured_dir),
                'unstructured_data': str(self.unstructured_dir),
                'csv_files': list(self.structured_dir.glob("*.csv")),
                'text_files': {
                    'clinical_notes': len(list((self.unstructured_dir / "clinical_notes").glob("*.txt"))),
                    'radiology_reports': len(list((self.unstructured_dir / "radiology_reports").glob("*.txt")))
                }
            }
        }
        
        # Save metadata
        metadata_file = self.output_dir / "dataset_metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        print(f"   Generated metadata file: {metadata_file}")


def main():
    """Main function to run data generation."""
    parser = argparse.ArgumentParser(description="Generate TCH Patient 360 PoC dataset")
    parser.add_argument("--patients", type=int, default=500000, 
                       help="Number of patients to generate (default: 500000)")
    parser.add_argument("--encounters", type=int, default=5,
                       help="Average encounters per patient (default: 5)")
    parser.add_argument("--output-dir", type=str, default="data/mock_data",
                       help="Output directory for generated data")
    parser.add_argument("--seed", type=int, default=42,
                       help="Random seed for reproducible data generation")
    parser.add_argument("--test-run", action="store_true",
                       help="Generate small test dataset (1000 patients)")
    parser.add_argument("--compress", action="store_true",
                       help="Compress output files with gzip (recommended for faster uploads)")
    
    args = parser.parse_args()
    
    # Adjust for test run
    if args.test_run:
        args.patients = 1000
        args.encounters = 3
        print("🧪 Running in test mode with smaller dataset")
    
    print("🏥 Texas Children's Hospital Patient 360 PoC Data Generator")
    print("=" * 60)
    
    # Initialize orchestrator
    orchestrator = TCHDataGenerationOrchestrator(
        output_dir=args.output_dir,
        seed=args.seed,
        compress_files=args.compress
    )
    
    # Generate complete dataset
    start_time = datetime.now()
    stats = orchestrator.generate_complete_dataset(
        num_patients=args.patients,
        encounters_per_patient=args.encounters
    )
    end_time = datetime.now()
    
    # Final summary
    duration = end_time - start_time
    print(f"\n⏱️  Generation completed in {duration}")
    print(f"📊 Total data points: {sum(stats.values()):,}")
    print(f"💾 Output directory: {Path(args.output_dir).absolute()}")
    
    print("\n🎉 TCH Patient 360 PoC dataset generation complete!")
    print("   Ready for import into Snowflake")


if __name__ == "__main__":
    # Need to import random at module level for the generators
    import random
    main()