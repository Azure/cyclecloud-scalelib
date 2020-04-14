from hpc.autoscale.hpctypes import VMFamily

VM_SIZES = {
    "Basic_A0": {"Family": "basicAFamily", "Name": "Basic_A0", "Sku": "A0"},
    "Basic_A1": {"Family": "basicAFamily", "Name": "Basic_A1", "Sku": "A1"},
    "Basic_A2": {"Family": "basicAFamily", "Name": "Basic_A2", "Sku": "A2"},
    "Basic_A3": {"Family": "basicAFamily", "Name": "Basic_A3", "Sku": "A3"},
    "Basic_A4": {"Family": "basicAFamily", "Name": "Basic_A4", "Sku": "A4"},
    "Standard_A0": {
        "Family": "standardA0_A7Family",
        "Name": "Standard_A0",
        "Sku": "A0",
    },
    "Standard_A1": {
        "Family": "standardA0_A7Family",
        "Name": "Standard_A1",
        "Sku": "A1",
    },
    "Standard_A10": {
        "Family": "standardA8_A11Family",
        "Name": "Standard_A10",
        "Sku": "A10",
    },
    "Standard_A11": {
        "Family": "standardA8_A11Family",
        "Name": "Standard_A11",
        "Sku": "A11",
    },
    "Standard_A1_v2": {
        "Family": "standardAv2Family",
        "Name": "Standard_A1_v2",
        "Sku": "A1_v2",
    },
    "Standard_A2": {
        "Family": "standardA0_A7Family",
        "Name": "Standard_A2",
        "Sku": "A2",
    },
    "Standard_A2_v2": {
        "Family": "standardAv2Family",
        "Name": "Standard_A2_v2",
        "Sku": "A2_v2",
    },
    "Standard_A2m_v2": {
        "Family": "standardAv2Family",
        "Name": "Standard_A2m_v2",
        "Sku": "A2m_v2",
    },
    "Standard_A3": {
        "Family": "standardA0_A7Family",
        "Name": "Standard_A3",
        "Sku": "A3",
    },
    "Standard_A4": {
        "Family": "standardA0_A7Family",
        "Name": "Standard_A4",
        "Sku": "A4",
    },
    "Standard_A4_v2": {
        "Family": "standardAv2Family",
        "Name": "Standard_A4_v2",
        "Sku": "A4_v2",
    },
    "Standard_A4m_v2": {
        "Family": "standardAv2Family",
        "Name": "Standard_A4m_v2",
        "Sku": "A4m_v2",
    },
    "Standard_A5": {
        "Family": "standardA0_A7Family",
        "Name": "Standard_A5",
        "Sku": "A5",
    },
    "Standard_A6": {
        "Family": "standardA0_A7Family",
        "Name": "Standard_A6",
        "Sku": "A6",
    },
    "Standard_A7": {
        "Family": "standardA0_A7Family",
        "Name": "Standard_A7",
        "Sku": "A7",
    },
    "Standard_A8": {
        "Family": "standardA8_A11Family",
        "Name": "Standard_A8",
        "Sku": "A8",
    },
    "Standard_A8_v2": {
        "Family": "standardAv2Family",
        "Name": "Standard_A8_v2",
        "Sku": "A8_v2",
    },
    "Standard_A8m_v2": {
        "Family": "standardAv2Family",
        "Name": "Standard_A8m_v2",
        "Sku": "A8m_v2",
    },
    "Standard_A9": {
        "Family": "standardA8_A11Family",
        "Name": "Standard_A9",
        "Sku": "A9",
    },
    "Standard_B12ms": {
        "Family": "standardBSFamily",
        "Name": "Standard_B12ms",
        "Sku": "B12ms",
    },
    "Standard_B16ms": {
        "Family": "standardBSFamily",
        "Name": "Standard_B16ms",
        "Sku": "B16ms",
    },
    "Standard_B1ls": {
        "Family": "standardBSFamily",
        "Name": "Standard_B1ls",
        "Sku": "B1ls",
    },
    "Standard_B1ms": {
        "Family": "standardBSFamily",
        "Name": "Standard_B1ms",
        "Sku": "B1ms",
    },
    "Standard_B1s": {
        "Family": "standardBSFamily",
        "Name": "Standard_B1s",
        "Sku": "B1s",
    },
    "Standard_B20ms": {
        "Family": "standardBSFamily",
        "Name": "Standard_B20ms",
        "Sku": "B20ms",
    },
    "Standard_B2ms": {
        "Family": "standardBSFamily",
        "Name": "Standard_B2ms",
        "Sku": "B2ms",
    },
    "Standard_B2s": {
        "Family": "standardBSFamily",
        "Name": "Standard_B2s",
        "Sku": "B2s",
    },
    "Standard_B4ms": {
        "Family": "standardBSFamily",
        "Name": "Standard_B4ms",
        "Sku": "B4ms",
    },
    "Standard_B8ms": {
        "Family": "standardBSFamily",
        "Name": "Standard_B8ms",
        "Sku": "B8ms",
    },
    "Standard_D1": {"Family": "standardDFamily", "Name": "Standard_D1", "Sku": "D1"},
    "Standard_D11": {"Family": "standardDFamily", "Name": "Standard_D11", "Sku": "D11"},
    "Standard_D11_v2": {
        "Family": "standardDv2Family",
        "Name": "Standard_D11_v2",
        "Sku": "D11_v2",
    },
    "Standard_D11_v2_Promo": {
        "Family": "standardDv2PromoFamily",
        "Name": "Standard_D11_v2_Promo",
        "Sku": "D11_v2_Promo",
    },
    "Standard_D12": {"Family": "standardDFamily", "Name": "Standard_D12", "Sku": "D12"},
    "Standard_D12_v2": {
        "Family": "standardDv2Family",
        "Name": "Standard_D12_v2",
        "Sku": "D12_v2",
    },
    "Standard_D12_v2_Promo": {
        "Family": "standardDv2PromoFamily",
        "Name": "Standard_D12_v2_Promo",
        "Sku": "D12_v2_Promo",
    },
    "Standard_D13": {"Family": "standardDFamily", "Name": "Standard_D13", "Sku": "D13"},
    "Standard_D13_v2": {
        "Family": "standardDv2Family",
        "Name": "Standard_D13_v2",
        "Sku": "D13_v2",
    },
    "Standard_D13_v2_Promo": {
        "Family": "standardDv2PromoFamily",
        "Name": "Standard_D13_v2_Promo",
        "Sku": "D13_v2_Promo",
    },
    "Standard_D14": {"Family": "standardDFamily", "Name": "Standard_D14", "Sku": "D14"},
    "Standard_D14_v2": {
        "Family": "standardDv2Family",
        "Name": "Standard_D14_v2",
        "Sku": "D14_v2",
    },
    "Standard_D14_v2_Promo": {
        "Family": "standardDv2PromoFamily",
        "Name": "Standard_D14_v2_Promo",
        "Sku": "D14_v2_Promo",
    },
    "Standard_D15_v2": {
        "Family": "standardDv2Family",
        "Name": "Standard_D15_v2",
        "Sku": "D15_v2",
    },
    "Standard_D16_v3": {
        "Family": "standardDv3Family",
        "Name": "Standard_D16_v3",
        "Sku": "D16_v3",
    },
    "Standard_D16a_v4": {
        "Family": "standardDAv4Family",
        "Name": "Standard_D16a_v4",
        "Sku": "D16a_v4",
    },
    "Standard_D16as_v4": {
        "Family": "standardDASv4Family",
        "Name": "Standard_D16as_v4",
        "Sku": "D16as_v4",
    },
    "Standard_D16s_v3": {
        "Family": "standardDSv3Family",
        "Name": "Standard_D16s_v3",
        "Sku": "D16s_v3",
    },
    "Standard_D1_v2": {
        "Family": "standardDv2Family",
        "Name": "Standard_D1_v2",
        "Sku": "D1_v2",
    },
    "Standard_D2": {"Family": "standardDFamily", "Name": "Standard_D2", "Sku": "D2"},
    "Standard_D2_v2": {
        "Family": "standardDv2Family",
        "Name": "Standard_D2_v2",
        "Sku": "D2_v2",
    },
    "Standard_D2_v2_Promo": {
        "Family": "standardDv2PromoFamily",
        "Name": "Standard_D2_v2_Promo",
        "Sku": "D2_v2_Promo",
    },
    "Standard_D2_v3": {
        "Family": "standardDv3Family",
        "Name": "Standard_D2_v3",
        "Sku": "D2_v3",
    },
    "Standard_D2a_v4": {
        "Family": "standardDAv4Family",
        "Name": "Standard_D2a_v4",
        "Sku": "D2a_v4",
    },
    "Standard_D2as_v4": {
        "Family": "standardDASv4Family",
        "Name": "Standard_D2as_v4",
        "Sku": "D2as_v4",
    },
    "Standard_D2s_v3": {
        "Family": "standardDSv3Family",
        "Name": "Standard_D2s_v3",
        "Sku": "D2s_v3",
    },
    "Standard_D3": {"Family": "standardDFamily", "Name": "Standard_D3", "Sku": "D3"},
    "Standard_D32_v3": {
        "Family": "standardDv3Family",
        "Name": "Standard_D32_v3",
        "Sku": "D32_v3",
    },
    "Standard_D32a_v4": {
        "Family": "standardDAv4Family",
        "Name": "Standard_D32a_v4",
        "Sku": "D32a_v4",
    },
    "Standard_D32as_v4": {
        "Family": "standardDASv4Family",
        "Name": "Standard_D32as_v4",
        "Sku": "D32as_v4",
    },
    "Standard_D32s_v3": {
        "Family": "standardDSv3Family",
        "Name": "Standard_D32s_v3",
        "Sku": "D32s_v3",
    },
    "Standard_D3_v2": {
        "Family": "standardDv2Family",
        "Name": "Standard_D3_v2",
        "Sku": "D3_v2",
    },
    "Standard_D3_v2_Promo": {
        "Family": "standardDv2PromoFamily",
        "Name": "Standard_D3_v2_Promo",
        "Sku": "D3_v2_Promo",
    },
    "Standard_D4": {"Family": "standardDFamily", "Name": "Standard_D4", "Sku": "D4"},
    "Standard_D48_v3": {
        "Family": "standardDv3Family",
        "Name": "Standard_D48_v3",
        "Sku": "D48_v3",
    },
    "Standard_D48a_v4": {
        "Family": "standardDAv4Family",
        "Name": "Standard_D48a_v4",
        "Sku": "D48a_v4",
    },
    "Standard_D48as_v4": {
        "Family": "standardDASv4Family",
        "Name": "Standard_D48as_v4",
        "Sku": "D48as_v4",
    },
    "Standard_D48s_v3": {
        "Family": "standardDSv3Family",
        "Name": "Standard_D48s_v3",
        "Sku": "D48s_v3",
    },
    "Standard_D4_v2": {
        "Family": "standardDv2Family",
        "Name": "Standard_D4_v2",
        "Sku": "D4_v2",
    },
    "Standard_D4_v2_Promo": {
        "Family": "standardDv2PromoFamily",
        "Name": "Standard_D4_v2_Promo",
        "Sku": "D4_v2_Promo",
    },
    "Standard_D4_v3": {
        "Family": "standardDv3Family",
        "Name": "Standard_D4_v3",
        "Sku": "D4_v3",
    },
    "Standard_D4a_v4": {
        "Family": "standardDAv4Family",
        "Name": "Standard_D4a_v4",
        "Sku": "D4a_v4",
    },
    "Standard_D4as_v4": {
        "Family": "standardDASv4Family",
        "Name": "Standard_D4as_v4",
        "Sku": "D4as_v4",
    },
    "Standard_D4s_v3": {
        "Family": "standardDSv3Family",
        "Name": "Standard_D4s_v3",
        "Sku": "D4s_v3",
    },
    "Standard_D5_v2": {
        "Family": "standardDv2Family",
        "Name": "Standard_D5_v2",
        "Sku": "D5_v2",
    },
    "Standard_D5_v2_Promo": {
        "Family": "standardDv2PromoFamily",
        "Name": "Standard_D5_v2_Promo",
        "Sku": "D5_v2_Promo",
    },
    "Standard_D64_v3": {
        "Family": "standardDv3Family",
        "Name": "Standard_D64_v3",
        "Sku": "D64_v3",
    },
    "Standard_D64a_v4": {
        "Family": "standardDAv4Family",
        "Name": "Standard_D64a_v4",
        "Sku": "D64a_v4",
    },
    "Standard_D64as_v4": {
        "Family": "standardDASv4Family",
        "Name": "Standard_D64as_v4",
        "Sku": "D64as_v4",
    },
    "Standard_D64s_v3": {
        "Family": "standardDSv3Family",
        "Name": "Standard_D64s_v3",
        "Sku": "D64s_v3",
    },
    "Standard_D8_v3": {
        "Family": "standardDv3Family",
        "Name": "Standard_D8_v3",
        "Sku": "D8_v3",
    },
    "Standard_D8a_v4": {
        "Family": "standardDAv4Family",
        "Name": "Standard_D8a_v4",
        "Sku": "D8a_v4",
    },
    "Standard_D8as_v4": {
        "Family": "standardDASv4Family",
        "Name": "Standard_D8as_v4",
        "Sku": "D8as_v4",
    },
    "Standard_D8s_v3": {
        "Family": "standardDSv3Family",
        "Name": "Standard_D8s_v3",
        "Sku": "D8s_v3",
    },
    "Standard_D96a_v4": {
        "Family": "standardDAv4Family",
        "Name": "Standard_D96a_v4",
        "Sku": "D96a_v4",
    },
    "Standard_D96as_v4": {
        "Family": "standardDASv4Family",
        "Name": "Standard_D96as_v4",
        "Sku": "D96as_v4",
    },
    "Standard_DC1s_v2": {
        "Family": "standardDCSv2Family",
        "Name": "Standard_DC1s_v2",
        "Sku": "DC1s_v2",
    },
    "Standard_DC2s": {
        "Family": "standardDCSFamily",
        "Name": "Standard_DC2s",
        "Sku": "DC2s",
    },
    "Standard_DC2s_v2": {
        "Family": "standardDCSv2Family",
        "Name": "Standard_DC2s_v2",
        "Sku": "DC2s_v2",
    },
    "Standard_DC4s": {
        "Family": "standardDCSFamily",
        "Name": "Standard_DC4s",
        "Sku": "DC4s",
    },
    "Standard_DC4s_v2": {
        "Family": "standardDCSv2Family",
        "Name": "Standard_DC4s_v2",
        "Sku": "DC4s_v2",
    },
    "Standard_DC8_v2": {
        "Family": "standardDCSv2Family",
        "Name": "Standard_DC8_v2",
        "Sku": "DC8_v2",
    },
    "Standard_DS1": {
        "Family": "standardDSFamily",
        "Name": "Standard_DS1",
        "Sku": "DS1",
    },
    "Standard_DS11": {
        "Family": "standardDSFamily",
        "Name": "Standard_DS11",
        "Sku": "DS11",
    },
    "Standard_DS11-1_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS11-1_v2",
        "Sku": "DS11-1_v2",
    },
    "Standard_DS11_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS11_v2",
        "Sku": "DS11_v2",
    },
    "Standard_DS11_v2_Promo": {
        "Family": "standardDSv2PromoFamily",
        "Name": "Standard_DS11_v2_Promo",
        "Sku": "DS11_v2_Promo",
    },
    "Standard_DS12": {
        "Family": "standardDSFamily",
        "Name": "Standard_DS12",
        "Sku": "DS12",
    },
    "Standard_DS12-1_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS12-1_v2",
        "Sku": "DS12-1_v2",
    },
    "Standard_DS12-2_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS12-2_v2",
        "Sku": "DS12-2_v2",
    },
    "Standard_DS12_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS12_v2",
        "Sku": "DS12_v2",
    },
    "Standard_DS12_v2_Promo": {
        "Family": "standardDSv2PromoFamily",
        "Name": "Standard_DS12_v2_Promo",
        "Sku": "DS12_v2_Promo",
    },
    "Standard_DS13": {
        "Family": "standardDSFamily",
        "Name": "Standard_DS13",
        "Sku": "DS13",
    },
    "Standard_DS13-2_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS13-2_v2",
        "Sku": "DS13-2_v2",
    },
    "Standard_DS13-4_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS13-4_v2",
        "Sku": "DS13-4_v2",
    },
    "Standard_DS13_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS13_v2",
        "Sku": "DS13_v2",
    },
    "Standard_DS13_v2_Promo": {
        "Family": "standardDSv2PromoFamily",
        "Name": "Standard_DS13_v2_Promo",
        "Sku": "DS13_v2_Promo",
    },
    "Standard_DS14": {
        "Family": "standardDSFamily",
        "Name": "Standard_DS14",
        "Sku": "DS14",
    },
    "Standard_DS14-4_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS14-4_v2",
        "Sku": "DS14-4_v2",
    },
    "Standard_DS14-8_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS14-8_v2",
        "Sku": "DS14-8_v2",
    },
    "Standard_DS14_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS14_v2",
        "Sku": "DS14_v2",
    },
    "Standard_DS14_v2_Promo": {
        "Family": "standardDSv2PromoFamily",
        "Name": "Standard_DS14_v2_Promo",
        "Sku": "DS14_v2_Promo",
    },
    "Standard_DS15_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS15_v2",
        "Sku": "DS15_v2",
    },
    "Standard_DS1_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS1_v2",
        "Sku": "DS1_v2",
    },
    "Standard_DS2": {
        "Family": "standardDSFamily",
        "Name": "Standard_DS2",
        "Sku": "DS2",
    },
    "Standard_DS2_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS2_v2",
        "Sku": "DS2_v2",
    },
    "Standard_DS2_v2_Promo": {
        "Family": "standardDSv2PromoFamily",
        "Name": "Standard_DS2_v2_Promo",
        "Sku": "DS2_v2_Promo",
    },
    "Standard_DS3": {
        "Family": "standardDSFamily",
        "Name": "Standard_DS3",
        "Sku": "DS3",
    },
    "Standard_DS3_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS3_v2",
        "Sku": "DS3_v2",
    },
    "Standard_DS3_v2_Promo": {
        "Family": "standardDSv2PromoFamily",
        "Name": "Standard_DS3_v2_Promo",
        "Sku": "DS3_v2_Promo",
    },
    "Standard_DS4": {
        "Family": "standardDSFamily",
        "Name": "Standard_DS4",
        "Sku": "DS4",
    },
    "Standard_DS4_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS4_v2",
        "Sku": "DS4_v2",
    },
    "Standard_DS4_v2_Promo": {
        "Family": "standardDSv2PromoFamily",
        "Name": "Standard_DS4_v2_Promo",
        "Sku": "DS4_v2_Promo",
    },
    "Standard_DS5_v2": {
        "Family": "standardDSv2Family",
        "Name": "Standard_DS5_v2",
        "Sku": "DS5_v2",
    },
    "Standard_DS5_v2_Promo": {
        "Family": "standardDSv2PromoFamily",
        "Name": "Standard_DS5_v2_Promo",
        "Sku": "DS5_v2_Promo",
    },
    "Standard_E16-4s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E16-4s_v3",
        "Sku": "E16-4s_v3",
    },
    "Standard_E16-8s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E16-8s_v3",
        "Sku": "E16-8s_v3",
    },
    "Standard_E16_v3": {
        "Family": "standardEv3Family",
        "Name": "Standard_E16_v3",
        "Sku": "E16_v3",
    },
    "Standard_E16a_v4": {
        "Family": "standardEAv4Family",
        "Name": "Standard_E16a_v4",
        "Sku": "E16a_v4",
    },
    "Standard_E16as_v4": {
        "Family": "standardEASv4Family",
        "Name": "Standard_E16as_v4",
        "Sku": "E16as_v4",
    },
    "Standard_E16s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E16s_v3",
        "Sku": "E16s_v3",
    },
    "Standard_E20_v3": {
        "Family": "standardEv3Family",
        "Name": "Standard_E20_v3",
        "Sku": "E20_v3",
    },
    "Standard_E20a_v4": {
        "Family": "standardEAv4Family",
        "Name": "Standard_E20a_v4",
        "Sku": "E20a_v4",
    },
    "Standard_E20as_v4": {
        "Family": "standardEASv4Family",
        "Name": "Standard_E20as_v4",
        "Sku": "E20as_v4",
    },
    "Standard_E20s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E20s_v3",
        "Sku": "E20s_v3",
    },
    "Standard_E2_v3": {
        "Family": "standardEv3Family",
        "Name": "Standard_E2_v3",
        "Sku": "E2_v3",
    },
    "Standard_E2a_v4": {
        "Family": "standardEAv4Family",
        "Name": "Standard_E2a_v4",
        "Sku": "E2a_v4",
    },
    "Standard_E2as_v4": {
        "Family": "standardEASv4Family",
        "Name": "Standard_E2as_v4",
        "Sku": "E2as_v4",
    },
    "Standard_E2s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E2s_v3",
        "Sku": "E2s_v3",
    },
    "Standard_E32-16s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E32-16s_v3",
        "Sku": "E32-16s_v3",
    },
    "Standard_E32-8s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E32-8s_v3",
        "Sku": "E32-8s_v3",
    },
    "Standard_E32_v3": {
        "Family": "standardEv3Family",
        "Name": "Standard_E32_v3",
        "Sku": "E32_v3",
    },
    "Standard_E32a_v4": {
        "Family": "standardEAv4Family",
        "Name": "Standard_E32a_v4",
        "Sku": "E32a_v4",
    },
    "Standard_E32as_v4": {
        "Family": "standardEASv4Family",
        "Name": "Standard_E32as_v4",
        "Sku": "E32as_v4",
    },
    "Standard_E32s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E32s_v3",
        "Sku": "E32s_v3",
    },
    "Standard_E4-2s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E4-2s_v3",
        "Sku": "E4-2s_v3",
    },
    "Standard_E48_v3": {
        "Family": "standardEv3Family",
        "Name": "Standard_E48_v3",
        "Sku": "E48_v3",
    },
    "Standard_E48a_v4": {
        "Family": "standardEAv4Family",
        "Name": "Standard_E48a_v4",
        "Sku": "E48a_v4",
    },
    "Standard_E48as_v4": {
        "Family": "standardEASv4Family",
        "Name": "Standard_E48as_v4",
        "Sku": "E48as_v4",
    },
    "Standard_E48s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E48s_v3",
        "Sku": "E48s_v3",
    },
    "Standard_E4_v3": {
        "Family": "standardEv3Family",
        "Name": "Standard_E4_v3",
        "Sku": "E4_v3",
    },
    "Standard_E4a_v4": {
        "Family": "standardEAv4Family",
        "Name": "Standard_E4a_v4",
        "Sku": "E4a_v4",
    },
    "Standard_E4as_v4": {
        "Family": "standardEASv4Family",
        "Name": "Standard_E4as_v4",
        "Sku": "E4as_v4",
    },
    "Standard_E4s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E4s_v3",
        "Sku": "E4s_v3",
    },
    "Standard_E64-16s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E64-16s_v3",
        "Sku": "E64-16s_v3",
    },
    "Standard_E64-32s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E64-32s_v3",
        "Sku": "E64-32s_v3",
    },
    "Standard_E64_v3": {
        "Family": "standardEv3Family",
        "Name": "Standard_E64_v3",
        "Sku": "E64_v3",
    },
    "Standard_E64a_v4": {
        "Family": "standardEAv4Family",
        "Name": "Standard_E64a_v4",
        "Sku": "E64a_v4",
    },
    "Standard_E64as_v4": {
        "Family": "standardEASv4Family",
        "Name": "Standard_E64as_v4",
        "Sku": "E64as_v4",
    },
    "Standard_E64i_v3": {
        "Family": "standardEIv3Family",
        "Name": "Standard_E64i_v3",
        "Sku": "E64i_v3",
    },
    "Standard_E64is_v3": {
        "Family": "standardEISv3Family",
        "Name": "Standard_E64is_v3",
        "Sku": "E64is_v3",
    },
    "Standard_E64s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E64s_v3",
        "Sku": "E64s_v3",
    },
    "Standard_E8-2s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E8-2s_v3",
        "Sku": "E8-2s_v3",
    },
    "Standard_E8-4s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E8-4s_v3",
        "Sku": "E8-4s_v3",
    },
    "Standard_E8_v3": {
        "Family": "standardEv3Family",
        "Name": "Standard_E8_v3",
        "Sku": "E8_v3",
    },
    "Standard_E8a_v4": {
        "Family": "standardEAv4Family",
        "Name": "Standard_E8a_v4",
        "Sku": "E8a_v4",
    },
    "Standard_E8as_v4": {
        "Family": "standardEASv4Family",
        "Name": "Standard_E8as_v4",
        "Sku": "E8as_v4",
    },
    "Standard_E8s_v3": {
        "Family": "standardESv3Family",
        "Name": "Standard_E8s_v3",
        "Sku": "E8s_v3",
    },
    "Standard_E96a_v4": {
        "Family": "standardEAv4Family",
        "Name": "Standard_E96a_v4",
        "Sku": "E96a_v4",
    },
    "Standard_E96as_v4": {
        "Family": "standardEASv4Family",
        "Name": "Standard_E96as_v4",
        "Sku": "E96as_v4",
    },
    "Standard_F1": {"Family": "standardFFamily", "Name": "Standard_F1", "Sku": "F1"},
    "Standard_F16": {"Family": "standardFFamily", "Name": "Standard_F16", "Sku": "F16"},
    "Standard_F16s": {
        "Family": "standardFSFamily",
        "Name": "Standard_F16s",
        "Sku": "F16s",
    },
    "Standard_F16s_v2": {
        "Family": "standardFSv2Family",
        "Name": "Standard_F16s_v2",
        "Sku": "F16s_v2",
    },
    "Standard_F1s": {
        "Family": "standardFSFamily",
        "Name": "Standard_F1s",
        "Sku": "F1s",
    },
    "Standard_F2": {"Family": "standardFFamily", "Name": "Standard_F2", "Sku": "F2"},
    "Standard_F2s": {
        "Family": "standardFSFamily",
        "Name": "Standard_F2s",
        "Sku": "F2s",
    },
    "Standard_F2s_v2": {
        "Family": "standardFSv2Family",
        "Name": "Standard_F2s_v2",
        "Sku": "F2s_v2",
    },
    "Standard_F32s_v2": {
        "Family": "standardFSv2Family",
        "Name": "Standard_F32s_v2",
        "Sku": "F32s_v2",
    },
    "Standard_F4": {"Family": "standardFFamily", "Name": "Standard_F4", "Sku": "F4"},
    "Standard_F48s_v2": {
        "Family": "standardFSv2Family",
        "Name": "Standard_F48s_v2",
        "Sku": "F48s_v2",
    },
    "Standard_F4s": {
        "Family": "standardFSFamily",
        "Name": "Standard_F4s",
        "Sku": "F4s",
    },
    "Standard_F4s_v2": {
        "Family": "standardFSv2Family",
        "Name": "Standard_F4s_v2",
        "Sku": "F4s_v2",
    },
    "Standard_F64s_v2": {
        "Family": "standardFSv2Family",
        "Name": "Standard_F64s_v2",
        "Sku": "F64s_v2",
    },
    "Standard_F72s_v2": {
        "Family": "standardFSv2Family",
        "Name": "Standard_F72s_v2",
        "Sku": "F72s_v2",
    },
    "Standard_F8": {"Family": "standardFFamily", "Name": "Standard_F8", "Sku": "F8"},
    "Standard_F8s": {
        "Family": "standardFSFamily",
        "Name": "Standard_F8s",
        "Sku": "F8s",
    },
    "Standard_F8s_v2": {
        "Family": "standardFSv2Family",
        "Name": "Standard_F8s_v2",
        "Sku": "F8s_v2",
    },
    "Standard_G1": {"Family": "standardGFamily", "Name": "Standard_G1", "Sku": "G1"},
    "Standard_G2": {"Family": "standardGFamily", "Name": "Standard_G2", "Sku": "G2"},
    "Standard_G3": {"Family": "standardGFamily", "Name": "Standard_G3", "Sku": "G3"},
    "Standard_G4": {"Family": "standardGFamily", "Name": "Standard_G4", "Sku": "G4"},
    "Standard_G5": {"Family": "standardGFamily", "Name": "Standard_G5", "Sku": "G5"},
    "Standard_GS1": {
        "Family": "standardGSFamily",
        "Name": "Standard_GS1",
        "Sku": "GS1",
    },
    "Standard_GS2": {
        "Family": "standardGSFamily",
        "Name": "Standard_GS2",
        "Sku": "GS2",
    },
    "Standard_GS3": {
        "Family": "standardGSFamily",
        "Name": "Standard_GS3",
        "Sku": "GS3",
    },
    "Standard_GS4": {
        "Family": "standardGSFamily",
        "Name": "Standard_GS4",
        "Sku": "GS4",
    },
    "Standard_GS4-4": {
        "Family": "standardGSFamily",
        "Name": "Standard_GS4-4",
        "Sku": "GS4-4",
    },
    "Standard_GS4-8": {
        "Family": "standardGSFamily",
        "Name": "Standard_GS4-8",
        "Sku": "GS4-8",
    },
    "Standard_GS5": {
        "Family": "standardGSFamily",
        "Name": "Standard_GS5",
        "Sku": "GS5",
    },
    "Standard_GS5-16": {
        "Family": "standardGSFamily",
        "Name": "Standard_GS5-16",
        "Sku": "GS5-16",
    },
    "Standard_GS5-8": {
        "Family": "standardGSFamily",
        "Name": "Standard_GS5-8",
        "Sku": "GS5-8",
    },
    "Standard_H16": {"Family": "standardHFamily", "Name": "Standard_H16", "Sku": "H16"},
    "Standard_H16_Promo": {
        "Family": "standardHPromoFamily",
        "Name": "Standard_H16_Promo",
        "Sku": "H16_Promo",
    },
    "Standard_H16m": {
        "Family": "standardHFamily",
        "Name": "Standard_H16m",
        "Sku": "H16m",
    },
    "Standard_H16m_Promo": {
        "Family": "standardHPromoFamily",
        "Name": "Standard_H16m_Promo",
        "Sku": "H16m_Promo",
    },
    "Standard_H16mr": {
        "Family": "standardHFamily",
        "Name": "Standard_H16mr",
        "Sku": "H16mr",
    },
    "Standard_H16mr_Promo": {
        "Family": "standardHPromoFamily",
        "Name": "Standard_H16mr_Promo",
        "Sku": "H16mr_Promo",
    },
    "Standard_H16r": {
        "Family": "standardHFamily",
        "Name": "Standard_H16r",
        "Sku": "H16r",
    },
    "Standard_H16r_Promo": {
        "Family": "standardHPromoFamily",
        "Name": "Standard_H16r_Promo",
        "Sku": "H16r_Promo",
    },
    "Standard_H8": {"Family": "standardHFamily", "Name": "Standard_H8", "Sku": "H8"},
    "Standard_H8_Promo": {
        "Family": "standardHPromoFamily",
        "Name": "Standard_H8_Promo",
        "Sku": "H8_Promo",
    },
    "Standard_H8m": {"Family": "standardHFamily", "Name": "Standard_H8m", "Sku": "H8m"},
    "Standard_H8m_Promo": {
        "Family": "standardHPromoFamily",
        "Name": "Standard_H8m_Promo",
        "Sku": "H8m_Promo",
    },
    "Standard_HB120rs_v2": {
        "Family": "standardHBrsv2Family",
        "Name": "Standard_HB120rs_v2",
        "Sku": "HB120rs_v2",
    },
    "Standard_HB60rs": {
        "Family": "standardHBSFamily",
        "Name": "Standard_HB60rs",
        "Sku": "HB60rs",
    },
    "Standard_HC44rs": {
        "Family": "standardHCSFamily",
        "Name": "Standard_HC44rs",
        "Sku": "HC44rs",
    },
    "Standard_L16s": {
        "Family": "standardLSFamily",
        "Name": "Standard_L16s",
        "Sku": "L16s",
    },
    "Standard_L16s_v2": {
        "Family": "standardLSv2Family",
        "Name": "Standard_L16s_v2",
        "Sku": "L16s_v2",
    },
    "Standard_L32s": {
        "Family": "standardLSFamily",
        "Name": "Standard_L32s",
        "Sku": "L32s",
    },
    "Standard_L32s_v2": {
        "Family": "standardLSv2Family",
        "Name": "Standard_L32s_v2",
        "Sku": "L32s_v2",
    },
    "Standard_L48s_v2": {
        "Family": "standardLSv2Family",
        "Name": "Standard_L48s_v2",
        "Sku": "L48s_v2",
    },
    "Standard_L4s": {
        "Family": "standardLSFamily",
        "Name": "Standard_L4s",
        "Sku": "L4s",
    },
    "Standard_L64s_v2": {
        "Family": "standardLSv2Family",
        "Name": "Standard_L64s_v2",
        "Sku": "L64s_v2",
    },
    "Standard_L80s_v2": {
        "Family": "standardLSv2Family",
        "Name": "Standard_L80s_v2",
        "Sku": "L80s_v2",
    },
    "Standard_L8s": {
        "Family": "standardLSFamily",
        "Name": "Standard_L8s",
        "Sku": "L8s",
    },
    "Standard_L8s_v2": {
        "Family": "standardLSv2Family",
        "Name": "Standard_L8s_v2",
        "Sku": "L8s_v2",
    },
    "Standard_M128": {
        "Family": "standardMSFamily",
        "Name": "Standard_M128",
        "Sku": "M128",
    },
    "Standard_M128-32ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M128-32ms",
        "Sku": "M128-32ms",
    },
    "Standard_M128-64ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M128-64ms",
        "Sku": "M128-64ms",
    },
    "Standard_M128m": {
        "Family": "standardMSFamily",
        "Name": "Standard_M128m",
        "Sku": "M128m",
    },
    "Standard_M128ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M128ms",
        "Sku": "M128ms",
    },
    "Standard_M128s": {
        "Family": "standardMSFamily",
        "Name": "Standard_M128s",
        "Sku": "M128s",
    },
    "Standard_M16-4ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M16-4ms",
        "Sku": "M16-4ms",
    },
    "Standard_M16-8ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M16-8ms",
        "Sku": "M16-8ms",
    },
    "Standard_M16ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M16ms",
        "Sku": "M16ms",
    },
    "Standard_M208ms_v2": {
        "Family": "standardMSv2Family",
        "Name": "Standard_M208ms_v2",
        "Sku": "M208ms_v2",
    },
    "Standard_M208s_v2": {
        "Family": "standardMSv2Family",
        "Name": "Standard_M208s_v2",
        "Sku": "M208s_v2",
    },
    "Standard_M32-16ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M32-16ms",
        "Sku": "M32-16ms",
    },
    "Standard_M32-8ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M32-8ms",
        "Sku": "M32-8ms",
    },
    "Standard_M32ls": {
        "Family": "standardMSFamily",
        "Name": "Standard_M32ls",
        "Sku": "M32ls",
    },
    "Standard_M32ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M32ms",
        "Sku": "M32ms",
    },
    "Standard_M32ts": {
        "Family": "standardMSFamily",
        "Name": "Standard_M32ts",
        "Sku": "M32ts",
    },
    "Standard_M416ms_v2": {
        "Family": "standardMSv2Family",
        "Name": "Standard_M416ms_v2",
        "Sku": "M416ms_v2",
    },
    "Standard_M416s_v2": {
        "Family": "standardMSv2Family",
        "Name": "Standard_M416s_v2",
        "Sku": "M416s_v2",
    },
    "Standard_M64": {
        "Family": "standardMSFamily",
        "Name": "Standard_M64",
        "Sku": "M64",
    },
    "Standard_M64-16ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M64-16ms",
        "Sku": "M64-16ms",
    },
    "Standard_M64-32ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M64-32ms",
        "Sku": "M64-32ms",
    },
    "Standard_M64ls": {
        "Family": "standardMSFamily",
        "Name": "Standard_M64ls",
        "Sku": "M64ls",
    },
    "Standard_M64m": {
        "Family": "standardMSFamily",
        "Name": "Standard_M64m",
        "Sku": "M64m",
    },
    "Standard_M64ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M64ms",
        "Sku": "M64ms",
    },
    "Standard_M64s": {
        "Family": "standardMSFamily",
        "Name": "Standard_M64s",
        "Sku": "M64s",
    },
    "Standard_M8-2ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M8-2ms",
        "Sku": "M8-2ms",
    },
    "Standard_M8-4ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M8-4ms",
        "Sku": "M8-4ms",
    },
    "Standard_M8ms": {
        "Family": "standardMSFamily",
        "Name": "Standard_M8ms",
        "Sku": "M8ms",
    },
    "Standard_NC12": {
        "Family": "standardNCFamily",
        "Name": "Standard_NC12",
        "Sku": "NC12",
    },
    "Standard_NC12_Promo": {
        "Family": "standardNCPromoFamily",
        "Name": "Standard_NC12_Promo",
        "Sku": "NC12_Promo",
    },
    "Standard_NC12s_v2": {
        "Family": "standardNCSv2Family",
        "Name": "Standard_NC12s_v2",
        "Sku": "NC12s_v2",
    },
    "Standard_NC12s_v3": {
        "Family": "standardNCSv3Family",
        "Name": "Standard_NC12s_v3",
        "Sku": "NC12s_v3",
    },
    "Standard_NC24": {
        "Family": "standardNCFamily",
        "Name": "Standard_NC24",
        "Sku": "NC24",
    },
    "Standard_NC24_Promo": {
        "Family": "standardNCPromoFamily",
        "Name": "Standard_NC24_Promo",
        "Sku": "NC24_Promo",
    },
    "Standard_NC24r": {
        "Family": "standardNCFamily",
        "Name": "Standard_NC24r",
        "Sku": "NC24r",
    },
    "Standard_NC24r_Promo": {
        "Family": "standardNCPromoFamily",
        "Name": "Standard_NC24r_Promo",
        "Sku": "NC24r_Promo",
    },
    "Standard_NC24rs_v2": {
        "Family": "standardNCSv2Family",
        "Name": "Standard_NC24rs_v2",
        "Sku": "NC24rs_v2",
    },
    "Standard_NC24rs_v3": {
        "Family": "standardNCSv3Family",
        "Name": "Standard_NC24rs_v3",
        "Sku": "NC24rs_v3",
    },
    "Standard_NC24s_v2": {
        "Family": "standardNCSv2Family",
        "Name": "Standard_NC24s_v2",
        "Sku": "NC24s_v2",
    },
    "Standard_NC24s_v3": {
        "Family": "standardNCSv3Family",
        "Name": "Standard_NC24s_v3",
        "Sku": "NC24s_v3",
    },
    "Standard_NC6": {
        "Family": "standardNCFamily",
        "Name": "Standard_NC6",
        "Sku": "NC6",
    },
    "Standard_NC6_Promo": {
        "Family": "standardNCPromoFamily",
        "Name": "Standard_NC6_Promo",
        "Sku": "NC6_Promo",
    },
    "Standard_NC6s_v2": {
        "Family": "standardNCSv2Family",
        "Name": "Standard_NC6s_v2",
        "Sku": "NC6s_v2",
    },
    "Standard_NC6s_v3": {
        "Family": "standardNCSv3Family",
        "Name": "Standard_NC6s_v3",
        "Sku": "NC6s_v3",
    },
    "Standard_ND12s": {
        "Family": "standardNDSFamily",
        "Name": "Standard_ND12s",
        "Sku": "ND12s",
    },
    "Standard_ND24rs": {
        "Family": "standardNDSFamily",
        "Name": "Standard_ND24rs",
        "Sku": "ND24rs",
    },
    "Standard_ND24s": {
        "Family": "standardNDSFamily",
        "Name": "Standard_ND24s",
        "Sku": "ND24s",
    },
    "Standard_ND40rs_v2": {
        "Family": "standardNDSv2Family",
        "Name": "Standard_ND40rs_v2",
        "Sku": "ND40rs_v2",
    },
    "Standard_ND40s_v3": {
        "Family": "standardNDSv3Family",
        "Name": "Standard_ND40s_v3",
        "Sku": "ND40s_v3",
    },
    "Standard_ND6s": {
        "Family": "standardNDSFamily",
        "Name": "Standard_ND6s",
        "Sku": "ND6s",
    },
    "Standard_NP10s": {
        "Family": "standardNPSFamily",
        "Name": "Standard_NP10s",
        "Sku": "NP10s",
    },
    "Standard_NP20s": {
        "Family": "standardNPSFamily",
        "Name": "Standard_NP20s",
        "Sku": "NP20s",
    },
    "Standard_NP40s": {
        "Family": "standardNPSFamily",
        "Name": "Standard_NP40s",
        "Sku": "NP40s",
    },
    "Standard_NV12": {
        "Family": "standardNVFamily",
        "Name": "Standard_NV12",
        "Sku": "NV12",
    },
    "Standard_NV12_Promo": {
        "Family": "standardNVPromoFamily",
        "Name": "Standard_NV12_Promo",
        "Sku": "NV12_Promo",
    },
    "Standard_NV12s_v2": {
        "Family": "standardNVSv2Family",
        "Name": "Standard_NV12s_v2",
        "Sku": "NV12s_v2",
    },
    "Standard_NV12s_v3": {
        "Family": "standardNVSv3Family",
        "Name": "Standard_NV12s_v3",
        "Sku": "NV12s_v3",
    },
    "Standard_NV16as_v4": {
        "Family": "standardNVSv4Family",
        "Name": "Standard_NV16as_v4",
        "Sku": "NV16as_v4",
    },
    "Standard_NV24": {
        "Family": "standardNVFamily",
        "Name": "Standard_NV24",
        "Sku": "NV24",
    },
    "Standard_NV24_Promo": {
        "Family": "standardNVPromoFamily",
        "Name": "Standard_NV24_Promo",
        "Sku": "NV24_Promo",
    },
    "Standard_NV24s_v2": {
        "Family": "standardNVSv2Family",
        "Name": "Standard_NV24s_v2",
        "Sku": "NV24s_v2",
    },
    "Standard_NV24s_v3": {
        "Family": "standardNVSv3Family",
        "Name": "Standard_NV24s_v3",
        "Sku": "NV24s_v3",
    },
    "Standard_NV32as_v4": {
        "Family": "standardNVSv4Family",
        "Name": "Standard_NV32as_v4",
        "Sku": "NV32as_v4",
    },
    "Standard_NV48s_v3": {
        "Family": "standardNVSv3Family",
        "Name": "Standard_NV48s_v3",
        "Sku": "NV48s_v3",
    },
    "Standard_NV4as_v4": {
        "Family": "standardNVSv4Family",
        "Name": "Standard_NV4as_v4",
        "Sku": "NV4as_v4",
    },
    "Standard_NV6": {
        "Family": "standardNVFamily",
        "Name": "Standard_NV6",
        "Sku": "NV6",
    },
    "Standard_NV6_Promo": {
        "Family": "standardNVPromoFamily",
        "Name": "Standard_NV6_Promo",
        "Sku": "NV6_Promo",
    },
    "Standard_NV6s_v2": {
        "Family": "standardNVSv2Family",
        "Name": "Standard_NV6s_v2",
        "Sku": "NV6s_v2",
    },
    "Standard_NV8as_v4": {
        "Family": "standardNVSv4Family",
        "Name": "Standard_NV8as_v4",
        "Sku": "NV8as_v4",
    },
    "Standard_PB6s": {
        "Family": "standardPBSFamily",
        "Name": "Standard_PB6s",
        "Sku": "PB6s",
    },
}


def get_family(vm_size: str) -> VMFamily:
    return VMFamily(VM_SIZES.get(vm_size, {}).get("Family", "unknown"))
