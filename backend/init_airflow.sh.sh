#!/bin/bash
# Setup script to create directories and fix permissions

set -e

echo "==================================="
echo "Setting up Ominimo Pipeline"
echo "==================================="

# Create all required directories
echo "Creating directory structure..."
mkdir -p data/input/events/motor_policy
mkdir -p data/output/events/motor_policy
mkdir -p data/output/discards/motor_policy
mkdir -p data/output/archive/motor_policy
mkdir -p logs
mkdir -p airflow-data
mkdir -p metadata

echo "Setting permissions (chmod 777 for development)..."
# Set full permissions for all users (development only)
chmod -R 777 data
chmod -R 777 logs
chmod -R 777 airflow-data

echo "Verifying directory structure..."
ls -la data/
ls -la data/input/events/
ls -la data/output/

echo ""
echo "✓ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Start Docker containers: docker-compose up -d"
echo "2. Check logs: docker-compose logs -f"
echo "3. Access Spark UI: http://localhost:8080"
echo "4. Access Airflow UI: http://localhost:8793 (admin/admin)"
echo "5. Access API: http://localhost:8000/docs"
echo ""