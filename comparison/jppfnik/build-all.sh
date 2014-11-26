#!/bin/bash
INSTALL_PROJECTS="lein-jppfnik jppfnik-tools"
UBERJAR_PROJECTS="jppfnik-client jppfnik-node jppfnik-server"

for p in $INSTALL_PROJECTS
do
	echo -e "\nInstalling $p...\n" && cd "$p" &&	lein install && cd ".."
done

for p in $UBERJAR_PROJECTS
do
	echo -e "\nBuilding $p...\n" && cd "$p" && lein do clean, install &&	lein with-profile release do clean, uberjar && cd ".."
done

echo -e "\nBuilding feature-selection...\n" && cd "feature-selection-jppf" && lein do clean, uberjar && cd ".."

