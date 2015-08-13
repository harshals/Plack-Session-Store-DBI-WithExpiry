package Plack::Session::Store::DBI::WithExpiry;
use strict;
use warnings;
 
# Is there a notion of auto-expiry?
 
our $VERSION   = '0.10';
our $AUTHORITY = 'cpan:STEVAN';
 
use MIME::Base64 ();
use Storable ();
 
use parent 'Plack::Session::Store';
 
use Plack::Util::Accessor qw[ dbh get_dbh table_name serializer deserializer expires];
 
sub new {
    my ($class, %params) = @_;
 
    if (! $params{dbh} && ! $params{get_dbh}) {
        die "DBI instance or a callback was not available in the argument list";
    }
 
    $params{table_name}   ||= 'sessions';
    $params{serializer}   ||= 
        sub { MIME::Base64::encode_base64( Storable::nfreeze( $_[0] ) ) };
    $params{deserializer} ||= 
        sub { Storable::thaw( MIME::Base64::decode_base64( $_[0] ) ) };

	## set default expiry to two hours
	$params{expires} 	  ||= 7200;

    my $self = bless { %params }, $class;
    return $self;
}
 
sub _dbh {
    my $self =shift;
    ( exists $self->{get_dbh} ) ? $self->{get_dbh}->() : $self->{dbh};
}
 
sub fetch {
    my ($self, $session_id) = @_;

	$session_id = "session:$session_id" unless $session_id =~  m/^expires/  ;

    my $table_name = $self->{table_name};
    my $dbh = $self->_dbh;
    my $sth = $dbh->prepare_cached("SELECT session_data FROM $table_name WHERE id = ? and expires > ?");
    $sth->execute( $session_id , time);
    my ($data) = $sth->fetchrow_array();
    $sth->finish;
    return () unless $data;

    $data = $self->deserializer->( $data ); 
    $data->{__updated} = time(); 
    $self->update_expiry($session_id, $data) ;
    return $data;
}
 
sub store {
    my ($self, $session_id, $session) = @_;
    my $table_name = $self->{table_name};
	$session_id = "session:$session_id" unless $session_id =~  m/^expires/  ;
 
    # XXX To be honest, I feel like there should be a transaction 
    # call here.... but Catalyst didn't have it, so I'm not so sure
 
    my $sth = $self->_dbh->prepare_cached("SELECT 1 FROM $table_name WHERE id = ?");
    $sth->execute($session_id);
 
    # need to fetch. on some DBD's execute()'s return status and
    # rows() is not reliable
    my ($exists) = $sth->fetchrow_array(); 
 
    $sth->finish;
     
    $session->{__updated} = time();
    if ($exists) {
        my $sth = $self->_dbh->prepare_cached("UPDATE $table_name SET session_data = ?, expires = ? WHERE id = ?");
        $sth->execute( $self->serializer->($session), $self->get_expiry , $session_id );
    }
    else {
    	## set the expiry
        my $sth = $self->_dbh->prepare_cached("INSERT INTO $table_name (id, session_data, expires) VALUES (?, ?, ?)");
        $sth->execute( $session_id , $self->serializer->($session), $self->get_expiry);
    }
     
}

sub get_expiry {
	
	return time + shift->{expires};
} 
sub update_expiry {
	my ($self, $session_id, $data) = @_;
	$session_id = "session:$session_id" unless $session_id =~  m/^expires/  ;
    my $table_name = $self->{table_name};
	my $sth = $self->_dbh->prepare_cached("UPDATE $table_name SET expires = ?, session_data = ?  WHERE id = ?");
	$sth->execute( $self->get_expiry , $self->serializer->( $data), $session_id  );

}

sub remove {
    my ($self, $session_id) = @_;
	$session_id = "session:$session_id" unless $session_id =~  m/^expires/  ;
    my $table_name = $self->{table_name};
    my $sth = $self->_dbh->prepare_cached("DELETE FROM $table_name WHERE id = ?");
    $sth->execute( $session_id );
    $sth->finish;
}
 
1;
 
__END__
