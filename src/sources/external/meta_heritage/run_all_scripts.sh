#!/bin/bash

echo "Starting execution of all Python scripts..."
echo

scripts=(
    "B2B_event_participants.py"
    "brussels_cinemas.py"
    "brussels_museums.py"
    "brussels_theatres.py"
    "emilia_romagna_GLAM.py"
    "emilia_romagna_cinema.py"
    "emilia_romagna_places_of_cultural_interest.py"
    "emilia_romagna_points_of_interest.py"
    "emilia_romagna_theatre.py"
    "germany_tourism.py"
    "porto_points_of_interest.py"
    "porto_wine_tourism.py"
    "TMO_members.py"
    "vienna_castles.py"
    "vienna_hotels.py"
    "vienna_innovative_companies.py"
    "vienna_museums.py"
    "vienna_points_of_interest.py"
    "vienna_top_location.py"
    "vienna_tourist_location.py"
)

for script in "${scripts[@]}"; do
    echo "Running $script..."
    python3 "$script"
    if [ $? -ne 0 ]; then
        echo "Error running $script"
        exit 1
    fi
    echo "$script completed successfully."
    echo
done

echo "All scripts completed successfully!"