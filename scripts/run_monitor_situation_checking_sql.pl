#!/usr/bin/perl -w
use DBI;
use Getopt::Long; 
use Data::Dumper;
use strict;
use warnings;


# M = Monitor  : These are situations where increases in numbers are probably problematic data, but could theoretically be explained by rare biology occurrences. 
#                If the number of increases are larger than expected rarity of the biological occurrence, these should be investigated further. 


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

my $out = "./probable_errors_".$db_name."_".$year."_".$mon."_".$mday.".txt"; 
open (OUT,">$out") || die "Did not create $out"; 


my @sql_queries = (
"select 'Genomes_with_non_methionine_starting_methione_ending_proteins_count' query_name, current_date(), database() as db, count(*)
from ( 
select g.id, g.scientific_name, count(*) as cnt 
from Genome g inner join IsOwnerOf io on g.id = io.from_link 
inner join IsProteinFor ip on ip.to_link = io.to_link 
inner join ProteinSequence ps on ip.from_link = ps.id 
where ps.sequence not like 'M%' 
and ps.sequence like '%M' 
group by g.id, g.scientific_name 
having cnt > 0 
order by cnt ) subq"
,
"select 'CDS_with_non_methionine_starting_methione_ending_proteins_count' query_name, current_date(), database() as db, sum(cnt) 
from ( 
select g.id, g.scientific_name, count(*) as cnt 
from Genome g inner join IsOwnerOf io on g.id = io.from_link 
inner join IsProteinFor ip on ip.to_link = io.to_link 
inner join ProteinSequence ps on ip.from_link = ps.id 
where ps.sequence not like 'M%' 
and ps.sequence like '%M' 
group by g.id, g.scientific_name 
having cnt > 0 
order by cnt ) subq"
,
"select 'CDS_length_inconsistent_with_protein_length_count' query_name, current_date(),  database() as db, sum(cnt) 
from ( 
select substring_index(f.id,".",2), count(*) cnt 
from Feature f 
inner join IsProteinFor i on f.id = i.to_link 
inner join ProteinSequence p on p.id = i.from_link 
where f.feature_type = 'CDS' 
and f.sequence_length/3 != (length(p.sequence) + 1) 
and f.sequence_length/3 != length(p.sequence) 
group by substring_index(f.id,".",2)) subq"
,
"select 'CDS_Features_not_modulo3_count' query_name, current_date(), database() as db, count(*) 
from (select f.id, f.sequence_length from Feature f inner join IsOwnerOf i on f.id = i.to_link 
where feature_type = 'CDS' and f.sequence_length%3 != 0 order by f.id) subq"
,
"select 'Genomes_with_CDS_Features_not_modulo3_count' query_name, current_date(), database() as db, count(*) 
from ( 
select g.id genome_id, g.scientific_name, g.domain, g.prokaryotic, s.id, count(*) as cnt
from Feature f inner join IsOwnerOf i on f.id = i.to_link inner join Genome g on g.id = i.from_link 
inner join Submitted su on g.id = su.to_link inner join Source s on s.id = su.from_link
where feature_type = 'CDS' and f.sequence_length%3 != 0 group by g.id, g.scientific_name, g.domain, g.prokaryotic, s.id
having cnt > 0 
order by count(*) desc) subq"
,
"select 'Genomes_with_at_least_1%_of_CDS_Features_not_modulo3_count' query_name, current_date(), database() as db, count(*) 
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
order by ((r1.not_modulo3_count/r2.total_cds_count) * 100) desc) subq"
,
"select 'CDS_Features_without_functions_count' query_name, current_date(), database() as db, count(f.id)
from Feature f
where f.feature_type = 'CDS' and f.function = ''"
);

#print headers
print OUT "Query_Name\tDate\tDB\tCount\n";
foreach my $sql_query (@sql_queries)
{
    my $sth = $dbh->prepare($sql_query) or die "Unable to prepare query : $sql_query :".$dbh->errstr(); 
    $sth->execute or die "SQL Error: for query $sql_query : $DBI::errstr\n"; 
    my (@results) = $sth->fetchrow_array();
    print "Completed $results[0] ". localtime(time)."\n";
    print OUT join("\t",@results). "\n";
}

close OUT;
