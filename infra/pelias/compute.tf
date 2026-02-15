# Find the latest Ubuntu 24.04 ARM image
data "oci_core_images" "ubuntu_arm" {
  compartment_id           = local.compartment_id
  operating_system         = "Canonical Ubuntu"
  operating_system_version = "24.04"
  shape                    = var.instance_shape
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"

  filter {
    name   = "display_name"
    values = ["\\w*aarch64\\w*"]
    regex  = true
  }
}

# Compute Instance
resource "oci_core_instance" "pelias" {
  compartment_id      = local.compartment_id
  availability_domain = local.availability_domain
  display_name        = "pelias-geocoder"
  shape               = var.instance_shape

  shape_config {
    ocpus         = var.instance_ocpus
    memory_in_gbs = var.instance_memory_gb
  }

  source_details {
    source_type             = "image"
    source_id               = data.oci_core_images.ubuntu_arm.images[0].id
    boot_volume_size_in_gbs = var.boot_volume_gb
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.pelias.id
    assign_public_ip = true
    display_name     = "pelias-vnic"
  }

  metadata = {
    ssh_authorized_keys = file(var.ssh_public_key_path)
    user_data           = base64encode(file("${path.module}/cloud-init.yaml"))
  }

  # Prevent recreation when image updates
  lifecycle {
    ignore_changes = [source_details[0].source_id]
  }
}
