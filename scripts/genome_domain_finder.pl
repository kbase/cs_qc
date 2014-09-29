use Data::Dumper;
use DBI;
use Getopt::Long;
use strict;

my $db_name = undef;
my $db_user = undef;
my $db_pwd = undef;
my $domainless_only = undef;
my $domain_conflicts =undef;
my $all_rows = undef;
my $populate_domainless = undef;

my $usage = "This command requires the db_name, db_user, db_pwd, domainless_only, all_rows, domain_conflicts and populate_domainless.\n".
    "The DB parameters need to be in single quotes.  \n".
    "domainless_only, all rows, domain_conflicts and populate_domainless are all integers (1=on, 0=off). \n".
    "domainless_only : If 1 it will only process for genomes without domains, otherwise it will do all genomes. \n".
    "all_rows : 1 will do tab delimited report of all rows processed, 0 no tab delimited report will be printed. \n".
    "domain_conflicts : will do report of all genomes where the populated domain in the genome table differs from the proposed domain. 1=report, 0= no report \n".
    "populate_domainless : will update the domainless genome records with the proposed domain.  1=populate, 0=do not populate (only running for reporting purposes).  \n".
    "Example Call : \n".
    "perl genome_domain_finder.pl -db_name='YOUR_DB_NAME' -db_user='YOUR_DB_USER' -db_pwd='YOUR_DB_PWD' -domainless_only=1 -all_rows=1 -domain_conflicts=1 -populate_domainless=0 \n";
(GetOptions('db_name=s' => \$db_name,
            'db_user=s' => \$db_user,
	    'db_pwd=s' => \$db_pwd,
            'domainless_only=i' => \$domainless_only,
	    'domain_conflicts=i' => \$domain_conflicts, 
	    'all_rows=i' => \$all_rows,
	    'populate_domainless=i' => \$populate_domainless
)
     && @ARGV == 0) || die $usage;
die $usage if ((!defined $db_user) || (!defined $db_pwd) || (!defined $db_name) || (!defined $domainless_only) 
	       || (!defined($domain_conflicts)) || (!defined($all_rows)) || (!defined($populate_domainless)));

my $full_db_name = 'DBI:mysql:'.$db_name;
my $dbh = DBI->connect($full_db_name,$db_user, $db_pwd, { RaiseError => 1, ShowErrorStatement => 1 } ); 

my $get_genome_list_q = qq^select g.id, g.scientific_name, t.scientific_name, t.id, i.confidence, g.domain
                           from Genome g inner join IsTaxonomyOf i on i.to_link = g.id
                           inner join TaxonomicGrouping t on i.from_link = t.id ^;
if ($domainless_only == 1)
{
    $get_genome_list_q .= qq^where g.domain = ''^;
}
my $get_genome_list_qh = $dbh->prepare($get_genome_list_q) or die "Unable to prepare get_genome_list_q : $get_genome_list_q  : ".$dbh->errstr();

my %domains_hash = (
    #'33090' => 'plants',
    '2' => 'Bacteria',
    '2157' => 'Archaea',
    '10239' => 'Viruses',
    '2759' => 'Eukaryota');

my %genome_info_hash ; #top level key is genome id  -> hash_ref {"genome_scientific_name"=>value,
                       #                                         "taxonomy_scientific_name"=>value,
                       #                                         "taxonomy_id"=>value,
                       #                                         "confidence"=>value,
                       #                                         "genome_domain"=>value,
                       #                                         "proposed_domain"=>value,                

$get_genome_list_qh->execute() or die "Unable toexecute get_genome_list_q : $get_genome_list_q  : ".$get_genome_list_qh->errstr(); 
while (my($genome_id,$g_scientific_name, $t_scientific_name, $t_id, $confidence, $g_domain) = $get_genome_list_qh->fetchrow_array())
{
#if ($genome_id eq 'kb|g.26797')
#{
    $genome_info_hash{$genome_id}={"genome_scientific_name"=>$g_scientific_name,
                                   "taxonomy_scientific_name"=>$t_scientific_name,
				   "taxonomy_id"=>$t_id,
                                   "confidence"=>$confidence,             
                                   "genome_domain"=>$g_domain};         
#}
}

my $get_parent_tax_id_q = qq^select from_link from IsGroupFor where to_link = ? ^;
my $get_parent_tax_id_qh = $dbh->prepare($get_parent_tax_id_q) or die "Unable to prepare get_parent_tax_id_q : $get_parent_tax_id_q : " . $dbh->errstr();

foreach my $genome_id (keys(%genome_info_hash))
{
    my $domain_found = undef;
    my $last_tax_id = $genome_info_hash{$genome_id}->{"taxonomy_id"};
    my $parent_tax_id = 1;

    while ((!defined($domain_found)) && (defined($parent_tax_id)))
    {
	$parent_tax_id = undef;
	$get_parent_tax_id_qh->execute($last_tax_id) or die "Unable to execute get_parent_tax_id_q $last_tax_id : $get_parent_tax_id_q : " . $get_parent_tax_id_qh->errstr();
	($parent_tax_id) = $get_parent_tax_id_qh->fetchrow_array();
	if (exists($domains_hash{$parent_tax_id}))
	{
	    $domain_found = $domains_hash{$parent_tax_id};
	}
	$last_tax_id = $parent_tax_id;
    }
    if (!defined($domain_found))
    {
	$domain_found = '';
    }
    $genome_info_hash{$genome_id}->{"proposed_domain"} = $domain_found;
}
#print Dumper(\%genome_info_hash);
my $header_row = "GENOME_ID\tGENOME_SCIENTIFIC_NAME\tTAX_SCIENTIFIC_NAME\tTAX_ID\tCONFIDENCE\tKBASE_DOMAIN\tPROPOSED_DOMAIN\n";
my $conflict_domains_count = 0;
my $genome_count = 0;
my $all_rows_result = '';
my $domain_conflict_report = '';
foreach my $genome_id (keys(%genome_info_hash))
{ 
    $all_rows_result .= $genome_id ."\t".
	$genome_info_hash{$genome_id}->{"genome_scientific_name"}."\t".
	$genome_info_hash{$genome_id}->{"taxonomy_scientific_name"}."\t".
	$genome_info_hash{$genome_id}->{"taxonomy_id"}."\t". 
	$genome_info_hash{$genome_id}->{"confidence"}."\t".
	$genome_info_hash{$genome_id}->{"genome_domain"}."\t".
	$genome_info_hash{$genome_id}->{"proposed_domain"}."\n";
    if (($genome_info_hash{$genome_id}->{"genome_domain"} ne '') && 
	($genome_info_hash{$genome_id}->{"genome_domain"} ne $genome_info_hash{$genome_id}->{"proposed_domain"}))
    {
	$conflict_domains_count++;
	$domain_conflict_report .= $genome_id ."\t".
	    $genome_info_hash{$genome_id}->{"genome_scientific_name"}."\t".
	    $genome_info_hash{$genome_id}->{"taxonomy_scientific_name"}."\t".
	    $genome_info_hash{$genome_id}->{"taxonomy_id"}."\t". 
	    $genome_info_hash{$genome_id}->{"confidence"}."\t".
	    $genome_info_hash{$genome_id}->{"genome_domain"}."\t".
	    $genome_info_hash{$genome_id}->{"proposed_domain"}."\n";
    }
    $genome_count++;
}

if ($all_rows == 1)
{
    print "ALL ROWS : \n".$header_row.$all_rows_result."\n\n";
}

if ($conflict_domains_count != 0 || $domain_conflicts == 1)
{
    print "\n\nNumber of rows with conflicting Domains : $conflict_domains_count \n";
}
if ($domain_conflicts == 1)
{
    print "\nCONFLICT DOMAINS : \n".$header_row.$domain_conflict_report."\n\n";
}
print "\n\nGenome : $genome_count \n";

my $genomes_updated_count = 0;
if ($populate_domainless == 1)
{
    #loop through all domainless genomes and update the genome domain to the proposed domain.
    print "\n\nUPDATING RESULTS\n";
    my $unable_to_update_count = 0;
    foreach my $genome_id (keys(%genome_info_hash))
    {
	my $update_genome_domain_q = qq^update Genome set domain = ? where id = ? ^;
        my $update_genome_domain_qh = $dbh->prepare($update_genome_domain_q) or die "Unable to prepare update_genome_domain_q : $update_genome_domain_q  : ".$dbh->errstr();
	if ($genome_info_hash{$genome_id}->{"genome_domain"} eq '')
	{
	    if($genome_info_hash{$genome_id}->{"proposed_domain"} ne '')
            {
		$update_genome_domain_qh->execute($genome_info_hash{$genome_id}->{"proposed_domain"},$genome_id) 
		    or die "unable to execute update_genome_domain_q with Domain : ".$genome_info_hash{$genome_id}->{"proposed_domain"}. " for Genome : ".$genome_id."\n";
		#print genome updated with proposed domain
		print "Updated Genome ".$genome_id." with the domain ". $genome_info_hash{$genome_id}->{"proposed_domain"} ."\n" ;
		$genomes_updated_count++;    
            }
            else
            {
		print "Unable to update $genome_id with a domain \n";
		$unable_to_update_count++;
            }
	}
    }
    print "\n\nTotal number of genomes that had their domain updated : $genomes_updated_count \n";
    print "Total domainless genomes unable to update with a domain : $unable_to_update_count \n\n";
}

exit(); 

