# VCN
resource "oci_core_vcn" "pelias" {
  compartment_id = local.compartment_id
  display_name   = "pelias-vcn"
  cidr_blocks    = ["10.0.0.0/16"]
  dns_label      = "pelias"
}

# Internet Gateway
resource "oci_core_internet_gateway" "pelias" {
  compartment_id = local.compartment_id
  vcn_id         = oci_core_vcn.pelias.id
  display_name   = "pelias-igw"
  enabled        = true
}

# Route Table (default route via internet gateway)
resource "oci_core_route_table" "pelias" {
  compartment_id = local.compartment_id
  vcn_id         = oci_core_vcn.pelias.id
  display_name   = "pelias-rt"

  route_rules {
    destination       = "0.0.0.0/0"
    network_entity_id = oci_core_internet_gateway.pelias.id
  }
}

# Security List
resource "oci_core_security_list" "pelias" {
  compartment_id = local.compartment_id
  vcn_id         = oci_core_vcn.pelias.id
  display_name   = "pelias-sl"

  # Allow all egress
  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
    stateless   = false
  }

  # SSH
  ingress_security_rules {
    protocol    = "6" # TCP
    source      = "0.0.0.0/0"
    stateless   = false
    description = "SSH"

    tcp_options {
      min = 22
      max = 22
    }
  }

  # Pelias API
  ingress_security_rules {
    protocol    = "6" # TCP
    source      = "0.0.0.0/0"
    stateless   = false
    description = "Pelias API"

    tcp_options {
      min = 4000
      max = 4000
    }
  }

  # ICMP (ping)
  ingress_security_rules {
    protocol    = "1" # ICMP
    source      = "0.0.0.0/0"
    stateless   = false
    description = "ICMP"
  }
}

# Public Subnet
resource "oci_core_subnet" "pelias" {
  compartment_id             = local.compartment_id
  vcn_id                     = oci_core_vcn.pelias.id
  display_name               = "pelias-subnet"
  cidr_block                 = "10.0.1.0/24"
  route_table_id             = oci_core_route_table.pelias.id
  security_list_ids          = [oci_core_security_list.pelias.id]
  prohibit_public_ip_on_vnic = false
  dns_label                  = "peliassub"
}
