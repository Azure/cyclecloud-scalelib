
[cluster example]
    FormLayout = selectionpanel
    Category = Infrastructure

    [[node defaults]]

        Credentials = $Credentials
        Region = $Region
        KeyPairLocation = ~/.ssh/cyclecloud.pem
        ImageName = cycle.image.centos7
        SubnetId = $ComputeSubnet
        MaxCoreCount = $MaxExecuteCoreCount

        [[[configuration autoscale.resources]]]
        location = $Region
        spot = false
        mpi = false
        infiniband = false

    [[nodearray htc]]
    MachineType = Standard_D4s_v3
        [[[configuration autoscale.resources]]]
        vm_size = Standard_D4s_v3
        memory = 16
        ncpus = 4
        pool = htc
        prototype = htc

    [[nodearray htcwestus]]
    MachineType = Standard_D4s_v3
    Region = westus
        [[[configuration autoscale.resources]]]
        vm_size = Standard_D4s_v3
        memory = 16
        ncpus = 4
        location = westus
        pool = htc
        prototype = htcwestus

    [[nodearray htcspot]]
        Extends = htc
        [[[configuration autoscale.resources]]]
        spot = true
        prototype = htcspot

    [[nodearray htcspotwestus]]
        Extends = htcspot
        Region = westus
        [[[configuration autoscale.resources]]]
        location = westus
        prototype = htcspotwestus
        
    [[nodearray highmem]]
    MachineType = Standard_E8s_v3
        [[[configuration autoscale.resources]]]
        vm_size = Standard_E8s_v3
        memory = 64
        ncpus = 8
        pool = highmem
        prototype = highmem

    [[nodearray gpu]]
    MachineType = Standard_NC12
        [[[configuration autoscale.resources]]]
        vm_size = Standard_NC12
        memory = 112
        ncpus = 12
        gpus = 2
        pool = gpu
        prototype = gpu

    [[nodearray mpi]]
    MachineType = Standard_A8
        [[[configuration autoscale.resources]]]
        vm_size = Standard_A8
        memory = 56
        ncpus = 8
        mpi = true
        infiniband = true
        pool = mpi
        prototype = mpi

[parameters Required Settings]
Order = 10

    [[parameters Cloud Service Provider Configuration]]
    Description = Configure the Cloud Provider account options.
    Order = 10

        [[[parameter Region]]]
        Label = Region
        Description = Deployment Location
        ParameterType = Cloud.Region

        [[[parameter MaxExecuteCoreCount]]]
        Label = Max Cores
        Description = The total number of execute cores to start
        DefaultValue = 500
        Config.Plugin = pico.form.NumberTextBox
        Config.MinValue = 1
        Config.MaxValue = 5000
        Config.IntegerOnly = true


    [[parameters Networking]]
    Description = Networking settings
    Order = 40

        [[[parameter ComputeSubnet]]]
        Label = Compute Subnet
        Description = Subnet Resource Path (ResourceGroup/VirtualNetwork/Subnet)
        Required = true
        ParameterType = Azure.Subnet

    [[parameters Azure Settings]]
    Description = Provider Account Name
    Order = 10 

        [[[parameter Credentials]]]
        Description = The credentials for the cloud provider
        ParameterType = Cloud.Credentials
