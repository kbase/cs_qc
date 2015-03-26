#!/usr/bin/perl -w
use DBI;
use Getopt::Long; 
use Data::Dumper;
use strict;
use warnings;

# P = Probable Error :  These are situations where the data is almost definitely wrong, but we already have data of this type.
#                       These are likely to be as a result of bad quality data or errors in the load scripts.  These need to be investigated further. 


my $db_name = undef; 
my $db_user = undef; 
my $db_pwd = undef; 
 
my $usage = "This command requires the db_name, db_user, db_pwd\n". 
    "The DB parameters need to be in single quotes.  \n". 
    "Example Call : \n". 
    "perl run_probable_error_checking_sql.pl -db_name='kbase_sapling_v4:db4.chicago.kbase.us' -db_user='YOUR_DB_USER' -db_pwd='YOUR_DB_PWD'\n"; 
(GetOptions('db_name=s' => \$db_name, 
            'db_user=s' => \$db_user, 
            'db_pwd=s' => \$db_pwd 
) 
 && @ARGV == 0) || die $usage; 
die $usage if ((!defined $db_user) || (!defined $db_pwd) || (!defined $db_name)); 


 
my $full_db_name = 'DBI:mysql:'.$db_name; 
my $dbh = DBI->connect($full_db_name,$db_user, $db_pwd, { RaiseError => 1, ShowErrorStatement => 1 } ); 

my ($db,$dummy) = split(':',$db_name);

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time); 
$year += 1900; 
$mon = $mon + 1;

my $out = "./probable_errors_".$db_name."_".$year."_".$mon."_".$mday.".txt"; 
open (OUT,">$out") || die "Did not create $out"; 


my @sql_queries = (
"select 'Genomes_with_a_CDS_length_too_large_and_big_for_protein_count' query_name, current_date(), database() as db, 'Way too large CDS', count(*)
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
order by count(*) desc) subq"
,
"select 'Protein_sequences_that_look_like_DNA_count' query_name, current_date(), database() as db, 'Protein sequences that are DNA only',count(*)
from (select p.id, p.sequence from ProteinSequence p where p.sequence RLIKE '^[AGTC]+\$') subq"
,
"select 'CDS_length_definitely_wrong_with_protein_length_count' query_name, current_date(),  database() as db,
'Query is geared to allow for stop codon being included or not. This would allow for up to 3 frameshifts (in same direction - deletion or insertion), which would be extremely unlikely   ', sum(cnt)
from ( 
select substring_index(f.id,'.',2), count(*) cnt
from Feature f
inner join IsProteinFor i on f.id = i.to_link
inner join ProteinSequence p on p.id = i.from_link 
where f.feature_type = 'CDS'
and FLOOR(f.sequence_length/3) != (length(p.sequence) + 1)
and CEIL(f.sequence_length/3) != (length(p.sequence) + 1) 
and FLOOR(f.sequence_length/3) != length(p.sequence) 
group by substring_index(f.id,'.',2)) subq"
,
"select 'Genomes_with_5percent_or_more_obviously_inconistent_proteins' query_name, current_date(), database() as db, 
'Genomes with 5% of CDS features having this case.  Red flag. Query is geared to allow for stop codon being included or not. This would allow for up to 3 frameshifts (in same direction - deletion or insertion), which would be extremely unlikely', 
count(*)
from ( 
select g.id, g.scientific_name, subq1.cnt, subq2.cds_cnt, (subq1.cnt/subq2.cds_cnt) 
from Genome g inner join
(select substring_index(f.id,'.',2) as genome_id, count(*) cnt
from Feature f 
inner join IsProteinFor i on f.id = i.to_link
inner join ProteinSequence p on p.id = i.from_link
where f.feature_type = 'CDS'
and FLOOR(f.sequence_length/3) != (length(p.sequence) + 1) 
and CEIL(f.sequence_length/3) != (length(p.sequence) + 1) 
and FLOOR(f.sequence_length/3) != length(p.sequence) 
group by substring_index(f.id,'.',2)) subq1 
on subq1.genome_id =g.id 
inner join (select substring_index(f1.id,'.',2) as genome_id, count(*) as cds_cnt from Feature f1 
where f1.feature_type = 'CDS' group by substring_index(f1.id,'.',2)) subq2 
on g.id = subq2.genome_id 
where (subq1.cnt/subq2.cds_cnt) > .05) fullsub"
,
"select 'CDS_Features_without_protein_sequences_count' query_name, current_date(), database() as db, 'CDS FEATURES SHOULD HAVE AN ASSOCIATED PROTEIN SEQUENCE', count(*) 
from (select f.id from
Feature f where feature_type = 'CDS' and id not in (select to_link from IsProteinFor)) subq"
,
"select 'CDS_length_equal_to_protein_length_count' query_name, current_date(), database() as db, 'CDS length equal to protein length', count(*) 
from (select f.id, f.sequence_length 
from Feature f 
inner join IsProteinFor i on f.id = i.to_link 
inner join ProteinSequence p on p.id = i.from_link 
where f.feature_type = 'CDS' 
and f.sequence_length = (length(p.sequence))) subq"
,
"select 'Genomes_without_IsTaxonomyOf_relationship_count' query_name, current_date(), database() as db, 
'The following are false positives (g.23746, g.26015, g.26860, g.2876, g.484, g.626, g.96) - This count should not be more than 7',
count(*) 
from Genome g 
where id not in (select to_link from IsTaxonomyOf)"
,
"select 'Genomes_with_suspicious_reversed_ordinals_count' query_name, current_date(), database() as db, 'Ordinals reversed', count(*) 
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
group by substring_index(s.feature,'.',2), g.scientific_name, g.domain, u.from_link, g.pegs) subq"
,
"select 'Protein_sequences_that_may_be_reversed_that_have_corresponding_features_count' query_name, current_date(), database() as db, 
'Ends in Methinonine and does not start with Methionine',count(*) 
from ( 
select distinct ps.id 
from ProteinSequence ps 
inner join IsProteinFor ip on ip.from_link = ps.id 
inner join Feature f on ip.to_link = f.id 
where sequence not like 'M%' 
and sequence like '%M') subq"
, 
"select 'Genomes_that_may_have_reversed_Protein_sequences_count' query_name, current_date(), database() as db, 
'Genomes with reversed proteins - Ends in Methinonine and does not start with Methionine',count(*) 
from ( 
select g.id, g.scientific_name, count(*) as cnt 
from Genome g inner join IsOwnerOf io on g.id = io.from_link 
inner join IsProteinFor ip on ip.to_link = io.to_link 
inner join ProteinSequence ps on ip.from_link = ps.id 
where ps.sequence not like 'M%' 
and ps.sequence like '%M' 
group by g.id, g.scientific_name 
having cnt > 0 
order by cnt) subq "
, 
"select 'Multiple_location_features_on_both_strands_count' query_name, current_date(), database() as db, 'Genes located on both strands', count(*) 
from (select distinct i1.from_link 
from IsLocatedIn i1 inner join IsLocatedIn i2 on i1.from_link = i2.from_link and i1.to_link = i2.to_link 
where i1.dir = '-' and i2.dir = '+') subq"
);

#print headers
print OUT "Query_Name\tDate\tDB\tComments\tCount\n";
foreach my $sql_query (@sql_queries)
{
    my $sth = $dbh->prepare($sql_query) or die "Unable to prepare query : $sql_query :".$dbh->errstr(); 
    $sth->execute or die "SQL Error: for query $sql_query : $DBI::errstr\n"; 
    my (@results) = $sth->fetchrow_array();
    print "Completed $results[0] ". localtime(time)."\n";
    print OUT join("\t",@results). "\n";
}

close OUT;
