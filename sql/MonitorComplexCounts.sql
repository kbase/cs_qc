#KEY INFORMATION
# M = Monitor  : These are situations where increases in numbers are probably problematic data, but could theoretically be explained by rare biology occurrences.  
#     		 If the number of increases are larger than expected rarity of the biological occurrence, these should be investigated further.
# 10 of these
# P = Probable Error :  These are situations where the data is almost definitely wrong, but we already have data of this type.  
#    	       	     	These are likely to be as a result of bad quality data or errors in the load scripts.  These need to be investigated further.
# 11 of these
# E = ERROR    : These are cases in which data appears to break the rules outlined by the CS schema.  These suggest a likely problem with the load or fix.  
#                New things that show up here need to be addressed before the data can be released.  Potentially a Jira ticket made.  Example: Genomes without contigs.
# 14 of these

#E
select 'Genomes_without_contigs_count' query_name, current_date(), database() as db, count(*) 
from (select g.id, count(c.id) as cnt
from Genome g left outer join IsComposedOf i on i.from_link = g.id
left outer join Contig c on c.id = i.to_link
group by g.id
having cnt = 0) no_contigs;

#E
select 'Genomes_with_inconsistent_contigs_count' query_name, current_date(), database() as db, count(*)
from (select g.id, g.contigs, count(c.id) as cnt
from Genome g left outer join IsComposedOf i on i.from_link = g.id
left outer join Contig c on c.id = i.to_link
group by g.id, g.contigs) as subq
where subq.contigs != subq.cnt;

#E
select 'Genomes_with_inconsistent_dna_size_to_contigs_length_sum_count' query_name, current_date(), database() as db, count(*)
from (
select g.id, g.dna_size, con_length.c_sum
from Genome g inner join
(select sum(cs.length) as c_sum, g1.id
from Genome g1 inner join IsComposedOf ic on ic.from_link = g1.id
inner join IsSequenceOf i on i.to_link = ic.to_link
inner join ContigSequence cs on cs.id = i.from_link
group by g1.id) as con_length on con_length.id = g.id
where g.dna_size != con_length.c_sum) subq;

#E
select 'Genomes_without_CDS_count' query_name, current_date(), database() as db, count(*) from 
(select g.id, su.from_link
from Genome g inner join Submitted su on g.id = su.to_link inner join Source s on s.id = su.from_link 
where g.id not in (select distinct g1.id 
from Genome g1 inner join IsOwnerOf i1 on g1.id = i1.from_link 
inner join Feature f1 on f1.id = i1.to_link 
where f1.feature_type = 'CDS')) subq ; 

#E
select 'Genomes_with_inconsistent_peg_and_cds_count' query_name, current_date(), database() as db, count(*)
from (
select g.id, g.pegs, count(*) cnt
from Genome g inner join IsOwnerOf io on io.from_link = g.id
inner join Feature f on f.id = io.to_link
where f.feature_type = 'CDS'
group by g.id, g.pegs
having g.pegs != cnt) subq;

#E
select 'CDS_Features_length_inconsistent_with_IsLocatedIn_length_sum_count' query_name, current_date(), database() as db, count(*) 
from 
(select f.id, f.sequence_length, sum.sum_length
from 
(select sum(len) as sum_length, from_link from IsLocatedIn group by from_link) sum 
inner join Feature f on f.id = sum.from_link 
where f.sequence_length != sum.sum_length 
and f.feature_type = 'CDS') subq; 

#P (SHOULD BE E, currently has count of 394)
select 'CDS_Features_without_protein_sequences_count' query_name, current_date(), database() as db, count(*) 
from (select f.id from
Feature f where feature_type = 'CDS' and id not in (select to_link from IsProteinFor)) subq; 

#M
select 'CDS_length_too_small_count' query_name, current_date(), database() as db, count(*) 
from (select f.id, f.sequence_length from Feature f where sequence_length < 10 and feature_type = 'CDS') subq; 

#M
select 'CDS_length_too_large_count' query_name, current_date(), database() as db, count(*) 
from (select f.id, f.sequence_length from Feature f where feature_type = 'CDS' and sequence_length > 100000) subq; 

#M
select 'Genomes_with_a_CDS_length_too_large_count' query_name, current_date(), database() as db, count(*) 
from (
select g.id, g.scientific_name, g.domain, g.prokaryotic, s.id as source, count(*) as cnt
from Feature f inner join IsOwnerOf i on f.id = i.to_link inner join Genome g on g.id = i.from_link
inner join Submitted su on g.id = su.to_link inner join Source s on s.id = su.from_link
where feature_type = 'CDS' and sequence_length > 100000 group by g.id, g.scientific_name, g.domain, g.prokaryotic, s.id
having cnt > 0
order by count(*) desc) subq;

#P
select 'Genomes_with_a_CDS_length_too_large_and_big_for_protein_count' query_name, current_date(), database() as db, count(*) 
from (
select g.id, g.scientific_name, g.domain, g.prokaryotic, s.id as source, count(*) as cnt
from Feature f inner join IsOwnerOf i on f.id = i.to_link inner join Genome g on g.id = i.from_link
inner join Submitted su on g.id = su.to_link inner join Source s on s.id = su.from_link
inner join IsProteinFor ip on f.id = ip.to_link 
inner join ProteinSequence p on p.id = ip.from_link
where feature_type = 'CDS' and sequence_length > 100000 
and sequence_length/3 > ((length(p.sequence) + 1)* 1.10)
group by g.id, g.scientific_name, g.domain, g.prokaryotic, s.id
having cnt > 0
order by count(*) desc) subq;

#M
select 'Genomes_with_non_methionine_starting_methione_ending_proteins_count' query_name, current_date(), database() as db, count(*)
from (
select g.id, g.scientific_name, count(*) as cnt
from Genome g inner join IsOwnerOf io on g.id = io.from_link
inner join IsProteinFor ip on ip.to_link = io.to_link
inner join ProteinSequence ps on ip.from_link = ps.id
where ps.sequence not like 'M%'
and ps.sequence like '%M'
group by g.id, g.scientific_name
having cnt > 0
order by cnt ) subq;

#M
select 'CDS_with_non_methionine_starting_methione_ending_proteins_count' query_name, current_date(), database() as db, sum(cnt)
from (
select g.id, g.scientific_name, count(*) as cnt
from Genome g inner join IsOwnerOf io on g.id = io.from_link
inner join IsProteinFor ip on ip.to_link = io.to_link
inner join ProteinSequence ps on ip.from_link = ps.id
where ps.sequence not like 'M%'
and ps.sequence like '%M'
group by g.id, g.scientific_name
having cnt > 0
order by cnt ) subq;

#P
select 'Protein_sequences_that_look_like_DNA_count' query_name, current_date(), database() as db, count(*) 
from (select p.id, p.sequence from ProteinSequence p where p.sequence RLIKE '^[AGTC]+$') subq; 

#M
select 'CDS_length_inconsistent_with_protein_length_count' query_name, current_date(),  database() as db, sum(cnt)
from (
select substring_index(f.id,".",2), count(*) cnt
from Feature f 
inner join IsProteinFor i on f.id = i.to_link
inner join ProteinSequence p on p.id = i.from_link
where f.feature_type = 'CDS' 
and f.sequence_length/3 != (length(p.sequence) + 1)
and f.sequence_length/3 != length(p.sequence)
group by substring_index(f.id,".",2)) subq; 

-- Query is geared to allow for stop codon being included or not.
-- This would allow for up to 3 frameshifts (in same direction - deletion or insertion), 
-- which would be extremely unlikely
#P
select 'CDS_length_definitely_wrong_with_protein_length_count' query_name, current_date(),  database() as db, sum(cnt)
from (
select substring_index(f.id,".",2), count(*) cnt
from Feature f 
inner join IsProteinFor i on f.id = i.to_link
inner join ProteinSequence p on p.id = i.from_link
where f.feature_type = 'CDS' 
and FLOOR(f.sequence_length/3) != (length(p.sequence) + 1)
and CEIL(f.sequence_length/3) != (length(p.sequence) + 1)
and FLOOR(f.sequence_length/3) != length(p.sequence)
group by substring_index(f.id,".",2)) subq;

-- Genomes with 5% of CDS features having this case.  Red flag.
-- Query is geared to allow for stop codon being included or not. 
-- This would allow for up to 3 frameshifts (in same direction - deletion or insertion), 
-- which would be extremely unlikely   
#P
select 'Genomes_with_5percent_or_more_obviously_inconistent_proteins' query_name, current_date(), database() as db, count(*) 
from (
select g.id, g.scientific_name, subq1.cnt, subq2.cds_cnt, (subq1.cnt/subq2.cds_cnt)
from Genome g inner join 
(select substring_index(f.id,".",2) as genome_id, count(*) cnt
from Feature f 
inner join IsProteinFor i on f.id = i.to_link
inner join ProteinSequence p on p.id = i.from_link
where f.feature_type = 'CDS' 
and FLOOR(f.sequence_length/3) != (length(p.sequence) + 1)
and CEIL(f.sequence_length/3) != (length(p.sequence) + 1)
and FLOOR(f.sequence_length/3) != length(p.sequence)
group by substring_index(f.id,".",2)) subq1 
on subq1.genome_id =g.id
inner join (select substring_index(f1.id,".",2) as genome_id, count(*) as cds_cnt from Feature f1 
where f1.feature_type = 'CDS' group by substring_index(f1.id,".",2)) subq2
on g.id = subq2.genome_id
where (subq1.cnt/subq2.cds_cnt) > .05) fullsub;

#P
select 'CDS_length_equal_to_protein_length_count' query_name, current_date(), database() as db, count(*) 
from (select f.id, f.sequence_length 
from Feature f 
inner join IsProteinFor i on f.id = i.to_link 
inner join ProteinSequence p on p.id = i.from_link 
where f.feature_type = 'CDS' 
and f.sequence_length = (length(p.sequence))) subq; 

#M
select 'CDS_Features_not_modulo3_count' query_name, current_date(), database() as db, count(*) 
from (select f.id, f.sequence_length from Feature f inner join IsOwnerOf i on f.id = i.to_link 
where feature_type = 'CDS' and f.sequence_length%3 != 0 order by f.id) subq; 

#M
select 'Genomes_with_CDS_Features_not_modulo3_count' query_name, current_date(), database() as db, count(*)
from (
select g.id genome_id, g.scientific_name, g.domain, g.prokaryotic, s.id, count(*) as cnt
from Feature f inner join IsOwnerOf i on f.id = i.to_link inner join Genome g on g.id = i.from_link 
inner join Submitted su on g.id = su.to_link inner join Source s on s.id = su.from_link
where feature_type = 'CDS' and f.sequence_length%3 != 0 group by g.id, g.scientific_name, g.domain, g.prokaryotic, s.id
having cnt > 0
order by count(*) desc) subq;

#M
select 'Genomes_with_at_least_1%_of_CDS_Features_not_modulo3_count' query_name, current_date(), database() as db, count(*)
from (
select r1.genome_id, ((r1.not_modulo3_count/r2.total_cds_count) * 100) percent_not_modulo3, r1.not_modulo3_count, r2.total_cds_count
from (select g1.id genome_id, count(f1.id) as not_modulo3_count
from Feature f1 inner join IsOwnerOf i1 on f1.id = i1.to_link inner join Genome g1 on g1.id = i1.from_link 
where f1.feature_type = 'CDS' and f1.sequence_length%3 != 0
group by g1.id) r1 inner join 
(select g2.id genome_id, count(f2.id) as total_cds_count
from Feature f2 inner join IsOwnerOf i2 on f2.id = i2.to_link inner join Genome g2 on g2.id = i2.from_link 
where f2.feature_type = 'CDS' 
group by g2.id) r2 on r1.genome_id = r2.genome_id
where ((r1.not_modulo3_count/r2.total_cds_count) * 100) > 1
order by ((r1.not_modulo3_count/r2.total_cds_count) * 100) desc) subq;

#E
select 'Pairings_without_component_features_count' query_name, current_date(), database() as db, count(*) 
from (select p.* from Pairing p where id not in (select to_link from IsInPair)) subq;

#E
select 'Missing_source_count' query_name, current_date(), database() as db, count(*) from (
select distinct r.from_link as ID
  from HasCompoundAliasFrom r where r.from_link not in 
  (select id from Source) 
union select distinct r.to_link 
  from HasReactionAliasFrom r where r.to_link not in 
  (select id from Source) 
union select distinct r.from_link 
  from AssertsFunctionFor r where r.from_link not in 
  (select id from Source) 
union select distinct r.from_link 
  from Aligned r where r.from_link not in 
  (select id from Source) 
union select distinct r.from_link 
  from Treed r where r.from_link not in 
  (select id from Source)) m;

#E
select 'Select_plasma_genomes_with_wrong_genetic_code_count' query_name, current_date(), database() as db, count(*) from (
select scientific_name, domain, genetic_code from Genome 
where (scientific_name like 'Acholeplasma%' or scientific_name like 'Mesoplasma%' or scientific_name like 'Mycoplasma%' 
or scientific_name like 'Spiroplasma%' or scientific_name like 'Ureaplasma%') 
and domain = 'Bacteria' and genetic_code != 4 order by scientific_name) m;
  
#E
select 'Eukaryotic_genomes_with_wrong_genetic_code_count' query_name, current_date(), database() as db, count(*) from (  
select scientific_name, genetic_code, domain from Genome where domain like 'Eukaryota%' and genetic_code != 1) m;

#E  
select 'Genomes_without_domains_count' query_name, current_date(), database() as db, count(*) from Genome where domain = '';

#P
select 'Genomes_without_IsTaxonomyOf_relationship_count' query_name, current_date(), database() as db, count(*) from Genome g 
where id not in (select to_link from IsTaxonomyOf);

-- Note there will be false positives with this query.  False positives will be features that cross the beginning and end junction
-- of circular contig.  Currently there are 7 known false positives. 
-- They are as follows:
-- | kb|g.23746                       | Bacteriophage SPP1                       | Viruses   | SEED      |   106 |        1 |
-- | kb|g.26015                       | Vibrio phage VSK                         | Viruses   | SEED      |    14 |        1 |
-- | kb|g.26860                       | Bacteriophage phig1e                     | Viruses   | SEED      |    50 |        1 |
-- | kb|g.2876                        | Escherichia coli TY-2482.contig.20110606 | Bacteria  | SEED      |  5141 |        2 |
-- | kb|g.484                         | Methanocaldococcus jannaschii DSM 2661   | Archaea   | SEED      |  1785 |        1 |
-- | kb|g.627                         | Pyrococcus abyssi GE5                    | Archaea   | SEED      |  1897 |        1 |
-- | kb|g.96                          | Synechocystis sp. PCC 6803               | Bacteria  | SEED      |  3468 |        1 |
-- If this number increases, the offending row (do inner query of this) should be examined.
#P
select 'Genomes_with_suspicious_reversed_ordinals_count' query_name, current_date(), database() as db, count(*) 
from (
select substring_index(s.feature,'.',2), g.scientific_name, g.domain, u.from_link, g.pegs, count(*) as cnt
from
(select distinct i1.from_link as feature
from IsLocatedIn i1 inner join IsLocatedIn i2 on i1.from_link = i2.from_link and i1.to_link = i2.to_link
where i1.dir = '-'
and i1.begin > i2.begin
and i1.ordinal > i2.ordinal) s
inner join Genome g on g.id = substring_index(s.feature,'.',2)
inner join Submitted u on u.to_link = g.id
group by substring_index(s.feature,'.',2), g.scientific_name, g.domain, u.from_link, g.pegs) subq;

#E
select 'Feature_with_duplicated_ordinals_count' query_name, current_date(), database() as db, count(*) 
from (
select count(*) as cnt, from_link, ordinal from IsLocatedIn group by from_link, ordinal having cnt > 1) subq;

#E
select 'Features_without_location_information_count' query_name, current_date(), database() as db, count(*) 
from (select id from Feature where id not in (select from_link from IsLocatedIn where ordinal = 0)) subq; 

#P
select 'Protein_sequences_that_may_be_reversed_that_have_corresponding_features_count' query_name, current_date(), database() as db, count(*) 
from (
select distinct ps.id
from ProteinSequence ps
inner join IsProteinFor ip on ip.from_link = ps.id
inner join Feature f on ip.to_link = f.id
where sequence not like 'M%'
and sequence like '%M') subq ;

#P
select 'Genomes_that_may_have_reversed_Protein_sequences_count' query_name, current_date(), database() as db, count(*) 
from (
select g.id, g.scientific_name, count(*) as cnt
from Genome g inner join IsOwnerOf io on g.id = io.from_link
inner join IsProteinFor ip on ip.to_link = io.to_link
inner join ProteinSequence ps on ip.from_link = ps.id
where ps.sequence not like 'M%'
and ps.sequence like '%M'
group by g.id, g.scientific_name
having cnt > 0
order by cnt) subq ;

#P
select 'Multiple_location_features_on_both_strands_count' query_name, current_date(), database() as db, count(*) 
from (select distinct i1.from_link
from IsLocatedIn i1 inner join IsLocatedIn i2 on i1.from_link = i2.from_link and i1.to_link = i2.to_link
where i1.dir = '-' and i2.dir = '+') subq;

#E
select 'Features_where_ordinal_count_does_not_correspond_to_number_of_location_count' query_name, current_date(), database() as db, count(*) 
from (select count(*) as cnt, max(ordinal) as maxOrd, from_link from IsLocatedIn group by from_link having cnt != (maxOrd + 1)) subq;

#M
select 'CDS_Features_without_functions_count' query_name, current_date(), database() as db, count(f.id)
from Feature f   
where f.feature_type = 'CDS' and f.function = '';