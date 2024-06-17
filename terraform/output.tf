output "RDS_instance_public_ip"{
    description = "The public address of the RDS instance"
    value = aws_db_instance.c11-punima-museum-db.endpoint
}