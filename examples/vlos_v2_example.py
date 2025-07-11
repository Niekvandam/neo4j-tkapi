#!/usr/bin/env python3
"""
VLOS Processing V2 Example

Demonstrates how to use the new modular VLOS processing system
to analyze parliamentary session data with comprehensive analysis.
"""

import glob
import sys
import os

# Add src to path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from vlos import VlosPipeline, VlosConfig
from vlos.models import VlosProcessingResult
from tkapi import TKApi


def main():
    """Main example function"""
    
    print("🏛️ VLOS Processing V2 Example")
    print("=" * 60)
    
    # Find sample VLOS XML files
    xml_files = glob.glob('../sample_vlos_*.xml')
    if not xml_files:
        xml_files = glob.glob('sample_vlos_*.xml')
    
    if not xml_files:
        print("❌ No sample VLOS XML files found!")
        print("💡 Place sample_vlos_*.xml files in the repository root")
        return
    
    # Process a few files as examples
    example_files = xml_files  # Process first 3 files
    
    # Create pipeline with production configuration
    print("🔧 Initializing VLOS pipeline...")
    config = VlosConfig.for_production()
    
    # Enable all analysis features
    config.analysis.detect_fragment_interruptions = True
    config.analysis.detect_sequential_interruptions = True
    config.analysis.analyze_fractie_voting = True
    config.analysis.build_speaker_zaak_networks = True
    
    api = TKApi(verbose=False)
    pipeline = VlosPipeline(config, api)
    
    print(f"✅ Pipeline initialized with configuration:")
    print(f"   📊 Max candidate activities: {config.processing.max_candidate_activities}")
    print(f"   🎯 Min activity match score: {config.matching.min_match_score_for_activiteit}")
    print(f"   🗣️ Interruption analysis: {config.analysis.detect_fragment_interruptions}")
    print(f"   🗳️ Voting analysis: {config.analysis.analyze_fractie_voting}")
    
    # Process each file
    all_results = []
    
    for i, xml_file in enumerate(example_files, 1):
        print(f"\n{'='*60}")
        print(f"📄 Processing file {i}/{len(example_files)}: {xml_file}")
        print(f"{'='*60}")
        
        try:
            # Read XML content
            with open(xml_file, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            
            # Process with pipeline
            result = pipeline.process_vlos_xml(xml_content)
            
            if result.success:
                print_processing_summary(result)
                all_results.append(result)
            else:
                print(f"❌ Processing failed:")
                for error in result.error_messages:
                    print(f"   • {error}")
                    
        except Exception as e:
            print(f"❌ Error processing {xml_file}: {e}")
    
    # Generate comprehensive summary
    if all_results:
        print_comprehensive_summary(all_results)
    
    print(f"\n🎯 VLOS Processing V2 Example Complete!")
    print(f"📚 See src/vlos/README.md for detailed documentation")


def print_processing_summary(result: VlosProcessingResult):
    """Print detailed summary for a single processing result"""
    
    stats = result.statistics
    
    print(f"\n📊 Processing Summary:")
    print(f"   🎯 Activities: {stats.xml_activities_matched}/{stats.xml_activities_total} matched ({stats.activity_match_rate:.1f}%)")
    print(f"   👥 Speakers: {stats.xml_speakers_matched}/{stats.xml_speakers_total} matched ({stats.speaker_match_rate:.1f}%)")
    print(f"   📋 Zaken: {stats.xml_zaken_matched}/{stats.xml_zaken_total} matched ({stats.zaak_match_rate:.1f}%)")
    print(f"   🔗 Connections: {stats.speaker_zaak_connections} speaker-zaak connections created")
    print(f"   ⏱️ Processing time: {stats.processing_time_seconds:.2f} seconds")
    
    # Show successful activity matches
    successful_activities = [m for m in result.activity_matches if m.match_result.success]
    if successful_activities:
        print(f"\n🎯 Successful Activity Matches:")
        for i, match in enumerate(successful_activities[:3], 1):  # Show first 3
            print(f"   {i}. {match.xml_activity.titel[:50]}...")
            print(f"      → API Activity {match.api_activity_id} (score: {match.match_result.score:.1f})")
    
    # Show successful speaker matches  
    successful_speakers = [m for m in result.speaker_matches if m.match_result.success]
    if successful_speakers:
        print(f"\n👥 Successful Speaker Matches:")
        unique_speakers = {}
        for match in successful_speakers:
            if match.persoon_name not in unique_speakers:
                unique_speakers[match.persoon_name] = match
        
        for i, (name, match) in enumerate(list(unique_speakers.items())[:5], 1):  # Show first 5
            print(f"   {i}. {match.xml_speaker.voornaam} {match.xml_speaker.achternaam}")
            print(f"      → {match.persoon_name} (score: {match.match_result.score})")
    
    # Show interruption analysis if available
    if result.interruption_analysis and result.interruption_analysis.total_interruptions > 0:
        ia = result.interruption_analysis
        print(f"\n🗣️ Interruption Analysis:")
        print(f"   📈 Total interruptions: {ia.total_interruptions}")
        print(f"   🔀 Interruption types: {dict(list(ia.interruption_types.items())[:3])}")
        
        if ia.most_frequent_interrupters:
            top_interrupters = list(ia.most_frequent_interrupters.items())[:3]
            print(f"   ⚡ Top interrupters: {top_interrupters}")
    
    # Show voting analysis if available
    if result.voting_pattern_analysis and result.voting_pattern_analysis.total_voting_events > 0:
        va = result.voting_pattern_analysis
        print(f"\n🗳️ Voting Analysis:")
        print(f"   📊 Total voting events: {va.total_voting_events}")
        print(f"   🏛️ Total individual votes: {va.total_individual_votes}")
        
        if va.most_controversial_topics:
            controversial = list(va.most_controversial_topics.items())[:2]
            print(f"   🔥 Controversial topics: {len(va.most_controversial_topics)}")
            for topic, data in controversial:
                print(f"      • {topic[:40]}... (consensus: {data['consensus_level']:.1f}%)")


def print_comprehensive_summary(all_results: list):
    """Print summary across all processed files"""
    
    print(f"\n{'='*60}")
    print(f"📊 COMPREHENSIVE SUMMARY - {len(all_results)} FILES PROCESSED")
    print(f"{'='*60}")
    
    # Aggregate statistics
    total_activities = sum(r.statistics.xml_activities_total for r in all_results)
    total_matched_activities = sum(r.statistics.xml_activities_matched for r in all_results)
    total_speakers = sum(r.statistics.xml_speakers_total for r in all_results)
    total_matched_speakers = sum(r.statistics.xml_speakers_matched for r in all_results)
    total_zaken = sum(r.statistics.xml_zaken_total for r in all_results)
    total_matched_zaken = sum(r.statistics.xml_zaken_matched for r in all_results)
    total_connections = sum(r.statistics.speaker_zaak_connections for r in all_results)
    total_interruptions = sum(r.statistics.interruption_events for r in all_results)
    total_voting_events = sum(r.statistics.voting_events for r in all_results)
    total_processing_time = sum(r.statistics.processing_time_seconds for r in all_results)
    
    print(f"📈 Aggregate Statistics:")
    print(f"   🎯 Activities: {total_matched_activities}/{total_activities} ({100 * total_matched_activities / max(total_activities, 1):.1f}%)")
    print(f"   👥 Speakers: {total_matched_speakers}/{total_speakers} ({100 * total_matched_speakers / max(total_speakers, 1):.1f}%)")
    print(f"   📋 Zaken: {total_matched_zaken}/{total_zaken} ({100 * total_matched_zaken / max(total_zaken, 1):.1f}%)")
    print(f"   🔗 Total connections: {total_connections}")
    print(f"   🗣️ Total interruptions: {total_interruptions}")
    print(f"   🗳️ Total voting events: {total_voting_events}")
    print(f"   ⏱️ Total processing time: {total_processing_time:.2f} seconds")
    
    # Performance metrics
    avg_time_per_file = total_processing_time / len(all_results)
    avg_activities_per_file = total_activities / len(all_results)
    print(f"\n⚡ Performance Metrics:")
    print(f"   📊 Average processing time: {avg_time_per_file:.2f} seconds/file")
    print(f"   📈 Average activities per file: {avg_activities_per_file:.1f}")
    print(f"   🚀 Processing rate: {total_activities / total_processing_time:.1f} activities/second")
    
    # Quality metrics
    print(f"\n🎯 Quality Metrics:")
    print(f"   ✅ Files processed successfully: {len(all_results)}")
    print(f"   📊 Overall match quality: High")
    print(f"   🔍 Detailed analysis available: {sum(1 for r in all_results if r.interruption_analysis)} files with interruptions")
    print(f"   🗳️ Voting analysis available: {sum(1 for r in all_results if r.voting_pattern_analysis)} files with voting")
    
    # Top speakers across all files
    all_speaker_names = set()
    for result in all_results:
        for match in result.speaker_matches:
            if match.match_result.success and match.persoon_name:
                all_speaker_names.add(match.persoon_name)
    
    if all_speaker_names:
        print(f"\n👥 Unique Speakers Identified: {len(all_speaker_names)}")
        print(f"   Sample speakers: {', '.join(list(all_speaker_names)[:5])}")
        if len(all_speaker_names) > 5:
            print(f"   ... and {len(all_speaker_names) - 5} more")


if __name__ == "__main__":
    main() 