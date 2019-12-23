wget -q https://packages.microsoft.com/config/ubuntu/19.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt update
sudo apt-get install dotnet-sdk-2.2
sudo apt update
sudo apt install git nuget snapd
sudo apt update
sudo snap install --classic code
sudo snap install gedit
sudo apt update
/snap/bin/code --install-extension fernandoescolar.vscode-solution-explorer
/snap/bin/code --install-extension ms-vscode.csharp
/snap/bin/code --install-extension craigthomas.supersharp
/snap/bin/code --install-extension doggy8088.netcore-extension-pack
/snap/bin/code --install-extension k--kato.docomment
/snap/bin/code --install-extension formulahendry.dotnet-test-explorer
/snap/bin/code --install-extension tintoy.msbuild-project-tools
/snap/bin/code --install-extension patcx.vscode-nuget-gallery
/snap/bin/code --install-extension eridem.vscode-nupkg
/snap/bin/code --install-extension mhutchie.git-graph
/snap/bin/code --install-extension rogalmic.vscode-xml-complete

