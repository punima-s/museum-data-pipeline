provider "aws" {
    region = var.AWS_REGION
    access_key = var.AWS_ACCESS_KEY
    secret_key = var.AWS_SECRET_KEY
}

# Describe resources
# resource "resource-type" "internal name" {
#   key = value
# }

resource "aws_s3_bucket" "example-bucket"{
    bucket = "c11-punima-test-bucket"
    force_destroy = true
}


data "aws_vpc" "c11-vpc" {
    id = "vpc-02112f9747d891585"
}

data "aws_db_subnet_group" "subnet-group"{
    name = "public_subnet_group_11"
}

resource "aws_security_group" "db-security-group"{
    name = "c11-punima-museum-scgroup"
    vpc_id = data.aws_vpc.c11-vpc.id

    egress {
        from_port        = 0
        to_port          = 0
        protocol         = "-1"
        cidr_blocks      = ["0.0.0.0/0"]
    }

    ingress {
        from_port = 5432
        to_port = 5432
        protocol = "tcp"
        cidr_blocks      = ["0.0.0.0/0"]
    }
}

resource "aws_db_instance" "c11-punima-museum-db" {
    allocated_storage            = 10
    db_name                      = "postgres"
    identifier                   = "c11-punima-museum-db"
    engine                       = "postgres"
    engine_version               = "16.1"
    instance_class               = "db.t3.micro"
    publicly_accessible          = true
    performance_insights_enabled = false
    skip_final_snapshot          = true
    db_subnet_group_name         = data.aws_db_subnet_group.subnet-group.name
    vpc_security_group_ids       = [aws_security_group.db-security-group.id]
    username                     = var.USERNAME
    password                     = var.PASSWORD
}