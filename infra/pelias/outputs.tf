output "instance_public_ip" {
  description = "Public IP address of the Pelias instance"
  value       = oci_core_instance.pelias.public_ip
}

output "pelias_api_url" {
  description = "URL for the Pelias API"
  value       = "http://${oci_core_instance.pelias.public_ip}:4000"
}

output "ssh_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh ubuntu@${oci_core_instance.pelias.public_ip}"
}

output "pelias_health_check" {
  description = "Command to check Pelias health"
  value       = "curl -s http://${oci_core_instance.pelias.public_ip}:4000/v1 | jq ."
}
