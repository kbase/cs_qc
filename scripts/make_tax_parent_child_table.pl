use Data::Dumper;
use DBI;
use Getopt::Long;
use strict;

my $db_name = undef;
my $db_user = undef;
my $db_pwd = undef;

my $usage = "This command requires the db_name, db_user, db_pwd\n".
    "The DB parameters need to be in single quotes.  \n".
    "Note this will delete the old TaxParentChild table and make a new one.\nExample Call : \n".
    "perl make_tax_parent_child_table.pl -db_name='kbase_sapling_v4:db4.chicago.kbase.us' -db_user='YOUR_DB_USER' -db_pwd='YOUR_DB_PWD'\n";
(GetOptions('db_name=s' => \$db_name,
            'db_user=s' => \$db_user,
	    'db_pwd=s' => \$db_pwd
)
     && @ARGV == 0) || die $usage;
die $usage if ((!defined $db_user) || (!defined $db_pwd) || (!defined $db_name)); 

my $full_db_name = 'DBI:mysql:'.$db_name;
my $dbh = DBI->connect($full_db_name,$db_user, $db_pwd, { RaiseError => 1, ShowErrorStatement => 1 } ); 

#DELETE OLD TABLE
my $delete_old_table_q = "DROP Table IF EXISTS TaxParentChild ";
my $delete_old_table_qh = $dbh->prepare($delete_old_table_q) or die "Unable to prepare drop TaxParentChild : ".$dbh->errstr();
$delete_old_table_qh->execute() or die "Unable to execute drop TaxParentChild : ".$delete_old_table_qh->errstr();

my $create_table_q = "CREATE TABLE IF NOT EXISTS TaxParentChild (
                          from_link  INT(10) unsigned NOT NULL,
                          to_link INT(10) unsigned NOT NULL,
                          distance INT(10) unsigned NOT NULL,
                          key(from_link),
                          key(to_link),
                          UNIQUE combined(from_link,to_link)
                          )";
my $create_table_qh = $dbh->prepare($create_table_q) or die "Unable to create TaxParentChild : ". $dbh->errstr();
$create_table_qh->execute() or die "Unable to create TaxParentChild : ". $create_table_qh->errstr();

#THE ROOT OF TAXONOMY TREE IS tax id 1
my @parent_tax_ids = (1);
make_tax_relationships($dbh,\@parent_tax_ids);

sub make_tax_relationships
{
    my $dbh = shift;
    my $parent_tax_ids_array_ref = shift;
    my @parent_tax_ids_array = @{$parent_tax_ids_array_ref};

    my $get_children_q = "select to_link from IsGroupFor where from_link = ? ";
    my $get_children_qh = $dbh->prepare($get_children_q) or die "Unable to prepare get_children_q : $get_children_q : ". $dbh->errstr();

    my $last_parent_id = $parent_tax_ids_array[-1];
    
    $get_children_qh->execute($last_parent_id) or die "Unable to execute get_children_q : $get_children_q $last_parent_id : ". $get_children_qh->errstr();
    my @children_ids;
    while (my($child_id) = $get_children_qh->fetchrow_array())
    {
	if ($child_id != $last_parent_id)
	{
	    my @temp_array = @parent_tax_ids_array;
	    push(@temp_array,$child_id);
	    push(@children_ids,$child_id);
	    make_tax_relationships($dbh,\@temp_array);
	}
    }
    if (scalar(@parent_tax_ids_array) > 1)
    {
	my @ancestor_ids = reverse(@parent_tax_ids_array[0..(scalar(@parent_tax_ids_array) - 2)]);
	
	my $insert_record_q = "Insert into TaxParentChild (from_link,to_link,distance) values(?,?,?)";
	my $insert_record_qh = $dbh->prepare($insert_record_q) or die "Unable to prepare insert_record_q : ". $dbh->errstr();
	
	for (my $i = 0; $i < scalar(@ancestor_ids); $i++)
	{
	    my $num = $i+1;
	    $insert_record_qh->execute($ancestor_ids[$i],$last_parent_id,$num)
		or die "Unable to execute insert_record_q : ". $insert_record_qh->errstr();
	}
    }
    return;
}
exit();
