"""
Voting Analyzer for VLOS Processing

Analyzes parliamentary voting patterns, fractie behavior, and consensus levels
to understand political dynamics and decision-making processes.
"""

from typing import List, Dict, Any
from collections import defaultdict

from ..config import VlosConfig
from ..models import (
    XmlVotingEvent, VotingAnalysis, VotingPatternAnalysis, ZaakMatch
)


class VotingAnalyzer:
    """Analyzes parliamentary voting patterns and consensus"""
    
    def __init__(self, config: VlosConfig):
        self.config = config
    
    def analyze_voting_in_activity(self, voting_events: List[XmlVotingEvent],
                                 activity_zaken: List[ZaakMatch],
                                 activity_id: str) -> List[VotingAnalysis]:
        """Analyze voting patterns within an activity"""
        
        if not self.config.analysis.analyze_fractie_voting:
            return []
        
        analyses = []
        
        for voting_event in voting_events:
            # Calculate vote breakdown
            vote_breakdown = defaultdict(list)
            for vote in voting_event.fractie_votes:
                vote_type = vote['vote_normalized']
                vote_breakdown[vote_type].append(vote['fractie'])
            
            # Calculate consensus level
            total_votes = len(voting_event.fractie_votes)
            if total_votes > 0:
                voor_count = len(vote_breakdown.get('voor', []))
                tegen_count = len(vote_breakdown.get('tegen', []))
                majority_count = max(voor_count, tegen_count)
                consensus_level = (majority_count / total_votes) * 100
            else:
                consensus_level = 0.0
            
            analysis = VotingAnalysis(
                voting_event=voting_event,
                activity_id=activity_id,
                topics_discussed=[z.xml_zaak.titel for z in activity_zaken if z.match_result.success],
                vote_breakdown=dict(vote_breakdown),
                consensus_level=consensus_level,
                total_votes=total_votes
            )
            analyses.append(analysis)
        
        return analyses
    
    def analyze_voting_patterns(self, all_voting_analyses: List[VotingAnalysis]) -> VotingPatternAnalysis:
        """Analyze comprehensive voting patterns across all activities"""
        
        if not all_voting_analyses:
            return VotingPatternAnalysis(
                total_voting_events=0,
                total_individual_votes=0,
                fractie_vote_counts={},
                fractie_alignment={},
                topic_vote_patterns={},
                vote_type_distribution={},
                most_controversial_topics={},
                unanimous_topics={}
            )
        
        # Track fractie voting behavior
        fractie_vote_counts = defaultdict(lambda: {
            'voor': 0, 'tegen': 0, 'onthouding': 0, 'niet_deelgenomen': 0, 'total': 0
        })
        
        fractie_topic_votes = defaultdict(lambda: defaultdict(lambda: {
            'voor': 0, 'tegen': 0, 'onthouding': 0
        }))
        
        # Track topic voting patterns
        topic_vote_patterns = defaultdict(lambda: {
            'votes': {'voor': [], 'tegen': [], 'onthouding': []},
            'consensus_level': 0,
            'total_votes': 0
        })
        
        # Vote type statistics
        vote_type_counts = defaultdict(int)
        
        # Process all voting analyses
        total_individual_votes = 0
        
        for analysis in all_voting_analyses:
            for vote in analysis.voting_event.fractie_votes:
                fractie = vote['fractie']
                vote_type = vote['vote_normalized']
                
                # Track overall fractie voting behavior
                if vote_type in fractie_vote_counts[fractie]:
                    fractie_vote_counts[fractie][vote_type] += 1
                fractie_vote_counts[fractie]['total'] += 1
                
                # Track fractie votes on specific topics
                for topic in analysis.topics_discussed:
                    if vote_type in fractie_topic_votes[fractie][topic]:
                        fractie_topic_votes[fractie][topic][vote_type] += 1
                
                # Track topic voting patterns
                for topic in analysis.topics_discussed:
                    if vote_type in topic_vote_patterns[topic]['votes']:
                        topic_vote_patterns[topic]['votes'][vote_type].append(fractie)
                    topic_vote_patterns[topic]['total_votes'] += 1
                
                # Overall vote type counting
                vote_type_counts[vote_type] += 1
                total_individual_votes += 1
        
        # Calculate consensus levels for topics
        for topic, data in topic_vote_patterns.items():
            total = data['total_votes']
            if total > 0:
                voor_count = len(data['votes']['voor'])
                tegen_count = len(data['votes']['tegen'])
                majority_count = max(voor_count, tegen_count)
                data['consensus_level'] = (majority_count / total) * 100
        
        # Calculate fractie alignment
        fractie_alignment = {}
        for fractie, counts in fractie_vote_counts.items():
            total_votes = counts['total']
            fractie_alignment[fractie] = {
                'total_votes': total_votes,
                'voor_percentage': (counts['voor'] / total_votes * 100) if total_votes > 0 else 0,
                'tegen_percentage': (counts['tegen'] / total_votes * 100) if total_votes > 0 else 0,
                'onthouding_percentage': (counts['onthouding'] / total_votes * 100) if total_votes > 0 else 0
            }
        
        # Identify controversial and unanimous topics
        if self.config.analysis.analyze_controversial_topics:
            controversial_topics = {
                k: v for k, v in topic_vote_patterns.items() 
                if v['consensus_level'] < 80 and v['total_votes'] > 0
            }
        else:
            controversial_topics = {}
        
        unanimous_topics = {
            k: v for k, v in topic_vote_patterns.items() 
            if v['consensus_level'] >= 95 and v['total_votes'] > 0
        }
        
        return VotingPatternAnalysis(
            total_voting_events=len(all_voting_analyses),
            total_individual_votes=total_individual_votes,
            fractie_vote_counts=dict(sorted(fractie_vote_counts.items(), 
                                          key=lambda x: x[1]['total'], reverse=True)),
            fractie_alignment=dict(sorted(fractie_alignment.items(), 
                                        key=lambda x: x[1]['voor_percentage'], reverse=True)),
            topic_vote_patterns=dict(sorted(topic_vote_patterns.items(), 
                                          key=lambda x: x[1]['consensus_level'], reverse=True)),
            vote_type_distribution=dict(vote_type_counts),
            most_controversial_topics=dict(sorted(controversial_topics.items(), 
                                                 key=lambda x: x[1]['consensus_level'])),
            unanimous_topics=dict(sorted(unanimous_topics.items(), 
                                       key=lambda x: x[1]['total_votes'], reverse=True))
        ) 