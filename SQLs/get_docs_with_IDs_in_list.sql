SELECT
    c
FROM
    metdata._default.MET_default AS c
WHERE
    c.ID in [
        ":V11.1.0:FV3_GSL_C384:20240801_000000:P900:SPFH:P900:S60:SL1L2",
        ":V11.1.0:FV3_GSL_C384:20240801_000000:P100:VGRD:P100:CONUS:SL1L2"
        ]