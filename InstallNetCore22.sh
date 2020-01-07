wget -q https://packages.microsoft.com/config/ubuntu/19.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt update
sudo apt-get install dotnet-sdk-2.2
sudo apt update
sudo apt install git nuget snapd
sudo apt update

