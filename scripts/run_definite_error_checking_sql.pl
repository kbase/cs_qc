#!/usr/bin/perl -w
use DBI;
use Getopt::Long; 
use Data::Dumper;
use strict;
use warnings;

# E = ERROR    : These are cases in which data appears to break the rules outlined by the CS schema.  These suggest a likely problem with the load or fix. 
#                New things that show up here need to be addressed before the data can be released.  Potentially a Jira ticket made.  Example: Genomes without contigs. 

my $db_name = undef; 
my $db_user = undef; 
my $db_pwd = undef; 
 
my $usage = "This command requires the db_name, db_user, db_pwd\n". 
    "The DB parameters need to be in single quotes.  \n". 
    "Example Call : \n". 
    "perl run_error_checking_sql.pl -db_name='kbase_sapling_v4:db4.chicago.kbase.us' -db_user='YOUR_DB_USER' -db_pwd='YOUR_DB_PWD'\n"; 
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

my $out = "./definite_errors_".$db_name."_".$year."_".$mon."_".$mday.".txt"; 
open (OUT,">$out") || die "Did not create $out"; 


my @sql_queries = (
"select 'Genomes_without_contigs_count' query_name, current_date(), database() as db, count(*) 
from (select g.id, count(c.id) as cnt 
from Genome g left outer join IsComposedOf i on i.from_link = g.id 
left outer join Contig c on c.id = i.to_link 
group by g.id 
having cnt = 0) no_contigs"
,
"select 'Genomes_with_inconsistent_contigs_count' query_name, current_date(), database() as db, count(*) 
from (select g.id, g.contigs, count(c.id) as cnt 
from Genome g left outer join IsComposedOf i on i.from_link = g.id 
left outer join Contig c on c.id = i.to_link 
group by g.id, g.contigs) as subq 
where subq.contigs != subq.cnt"
,
"select 'Genomes_without_CDS_count' query_name, current_date(), database() as db, count(*) from 
(select g.id, su.from_link 
from Genome g inner join Submitted su on g.id = su.to_link inner join Source s on s.id = su.from_link 
where g.id not in (select distinct g1.id 
from Genome g1 inner join IsOwnerOf i1 on g1.id = i1.from_link 
inner join Feature f1 on f1.id = i1.to_link 
where f1.feature_type = 'CDS')) subq"
,
"select 'Genomes_with_inconsistent_peg_and_cds_count' query_name, current_date(), database() as db, count(*) 
from ( 
select g.id, g.pegs, count(*) cnt 
from Genome g inner join IsOwnerOf io on io.from_link = g.id 
inner join Feature f on f.id = io.to_link 
where f.feature_type = 'CDS' 
group by g.id, g.pegs 
having g.pegs != cnt) subq"
,
"select 'CDS_Features_length_inconsistent_with_IsLocatedIn_length_sum_count' query_name, current_date(), database() as db, count(*) 
from 
(select f.id, f.sequence_length, sum.sum_length 
from 
(select sum(len) as sum_length, from_link from IsLocatedIn group by from_link) sum 
inner join Feature f on f.id = sum.from_link 
where f.sequence_length != sum.sum_length 
and f.feature_type = 'CDS') subq"
,
"select 'Pairings_without_component_features_count' query_name, current_date(), database() as db, count(*) 
from (select p.* from Pairing p where id not in (select to_link from IsInPair)) subq"
,
"select 'Missing_source_count' query_name, current_date(), database() as db, count(*) from ( 
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
    (select id from Source)) m"
,
"select 'Select_plasma_genomes_with_wrong_genetic_code_count' query_name, current_date(), database() as db, count(*) from ( 
select scientific_name, domain, genetic_code from Genome 
where (scientific_name like 'Acholeplasma%' or scientific_name like 'Mesoplasma%' or scientific_name like 'Mycoplasma%' 
or scientific_name like 'Spiroplasma%' or scientific_name like 'Ureaplasma%') 
and domain = 'Bacteria' and genetic_code != 4 order by scientific_name) m"
,
"select 'Eukaryotic_genomes_with_wrong_genetic_code_count' query_name, current_date(), database() as db, count(*) from ( 
select scientific_name, genetic_code, domain from Genome where domain like 'Eukaryota%' and genetic_code != 1) m"
, 
"select 'Genomes_without_domains_count' query_name, current_date(), database() as db, count(*) from Genome where domain = ''"
,
"select 'Feature_with_duplicated_ordinals_count' query_name, current_date(), database() as db, count(*) 
from ( 
select count(*) as cnt, from_link, ordinal from IsLocatedIn group by from_link, ordinal having cnt > 1) subq"
, 
"select 'Features_without_location_information_count' query_name, current_date(), database() as db, count(*) 
from (select id from Feature where id not in (select from_link from IsLocatedIn where ordinal = 0)) subq"
, 
"select 'Features_where_ordinal_count_does_not_correspond_to_number_of_location_count' query_name, current_date(), database() as db, count(*) 
from (select count(*) as cnt, max(ordinal) as maxOrd, from_link from IsLocatedIn group by from_link having cnt != (maxOrd + 1)) subq" 
);

#print headers
print OUT "Query_Name\tDate\tDB\tCount\n";
foreach my $sql_query (@sql_queries)
{
    my $sth = $dbh->prepare($sql_query) or die "Unable to prepare query : $sql_query :".$dbh->errstr(); 
    $sth->execute or die "SQL Error: for query $sql_query : $DBI::errstr\n"; 
    my (@results) = $sth->fetchrow_array();
    print "Completed $results[0] ". localtime(time)."\n";
    if ($results[3] > 0)
    {
	print OUT join("\t",@results). "\n";
    }
}

close OUT;
