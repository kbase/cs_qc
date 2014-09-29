use Data::Dumper;
use DBI;
use Getopt::Long;
use strict;

my $db_name = undef;
my $db_user = undef;
my $db_pwd = undef;
my $root_tax_id = undef;

my $usage = "This command requires the db_name, db_user, db_pwd, root_tax_id.\n".
    "The DB parameters need to be in single quotes.  \n".
    "The root_tax_id will get all genomes that match any of the children below theExample Call : \n".
    "perl get_children_genomes.pl -db_name='kbase_sapling_v4:db4.chicago.kbase.us' -db_user='YOUR_DB_USER' -db_pwd='YOUR_DB_PWD' -root_tax_id \n";
(GetOptions('db_name=s' => \$db_name,
            'db_user=s' => \$db_user,
	    'db_pwd=s' => \$db_pwd,
            'root_tax_id=i' => \$root_tax_id
)
     && @ARGV == 0) || die $usage;
die $usage if ((!defined $db_user) || (!defined $db_pwd) || (!defined $db_name) || (!defined $root_tax_id)); 

my $full_db_name = 'DBI:mysql:'.$db_name;
my $dbh = DBI->connect($full_db_name,$db_user, $db_pwd, { RaiseError => 1, ShowErrorStatement => 1 } ); 

my %full_tax_id_list = ($root_tax_id => 1);  #list of all tax ids found under root tax id
my @current_level_tax_ids = ($root_tax_id); #current children from current level of query

while (scalar(@current_level_tax_ids) > 0)
{
    my @current_parents= @current_level_tax_ids;
#print "\nCurrent Parents: ".Dumper(\@current_parents);
    @current_level_tax_ids = get_children_tax_ids($dbh,\@current_parents);
#print "\nCurrent Children: ".Dumper(\@current_level_tax_ids);
    foreach my $current_tax_id (@current_level_tax_ids)
    {
	$full_tax_id_list{$current_tax_id}=1;
    }
}

#print Dumper(\%full_tax_id_list);

my $get_genome_list_q = qq^select g.id, g.scientific_name, t.scientific_name, t.id, i.confidence, g.domain
                           from Genome g inner join IsTaxonomyOf i on i.to_link = g.id
                           inner join TaxonomicGrouping t on i.from_link = t.id 
                           where t.id in (^. 
    join(",", ("?") x keys(%full_tax_id_list)) . ") ";

my $get_genome_list_qh = $dbh->prepare($get_genome_list_q) or die "Unable to prepare get_genome_list_q : $get_genome_list_q  : ".$dbh->errstr();

my %genome_info_hash ; #top level key is genome id  -> hash_ref {"genome_scientific_name"=>value,
                       #                                         "taxonomy_scientific_name"=>value,
                       #                                         "taxonomy_id"=>value,
                       #                                         "confidence"=>value,
                       #                                         "genome_domain"=>value,
                       #                                         "proposed_domain"=>value,                

$get_genome_list_qh->execute(keys(%full_tax_id_list)) or die "Unable to execute get_genome_list_q : $get_genome_list_q  : ".$get_genome_list_qh->errstr(); 
while (my($genome_id,$g_scientific_name, $t_scientific_name, $t_id, $confidence, $g_domain) = $get_genome_list_qh->fetchrow_array())
{
    $genome_info_hash{$genome_id}={"genome_scientific_name"=>$g_scientific_name,
                                   "taxonomy_scientific_name"=>$t_scientific_name,
				   "taxonomy_id"=>$t_id,
                                   "confidence"=>$confidence,             
                                   "genome_domain"=>$g_domain};         
}

#print Dumper(\%genome_info_hash);
my $header_row = "GENOME_ID\tGENOME_SCIENTIFIC_NAME\tTAX_SCIENTIFIC_NAME\tTAX_ID\tCONFIDENCE\tKBASE_DOMAIN\n";
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
	$genome_info_hash{$genome_id}->{"genome_domain"}."\n";
    $genome_count++;
}

print "$genome_count results : \n".$header_row.$all_rows_result."\n\n";

exit(); 


sub get_children_tax_ids
{
    my $dbh = shift;
    my $parent_tax_ids_ref = shift;
    my @parent_tax_ids = @{$parent_tax_ids_ref};
    my $get_children_tax_id_q = qq^select to_link from IsGroupFor where from_link in (^.
	join(",", ("?") x @parent_tax_ids) . ") ";
    my $get_children_tax_id_qh = $dbh->prepare($get_children_tax_id_q) or die "Unable to prepare get_children_tax_id_q : $get_children_tax_id_q  : ".$dbh->errstr();
    $get_children_tax_id_qh->execute(@parent_tax_ids) or die "Unable to execute get_children_tax_id_q : $get_children_tax_id_q  : ".$get_children_tax_id_qh->errstr();
    my @children_tax_ids;
    while (my ($child_tax_id) = $get_children_tax_id_qh->fetchrow_array())
    {
	push(@children_tax_ids,$child_tax_id);
    }
    return @children_tax_ids;
}

