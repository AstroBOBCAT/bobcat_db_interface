CREATE DATABASE bobcat;

\c bobcat

CREATE TABLE candidate (
    name VARCHAR(50),
    ra_deg FLOAT8,
    dec_deg FLOAT8,
    redshift FLOAT8,
    obs_type_done VARCHAR(3) ARRAY,
    PRIMARY KEY(name)
);

CREATE TABLE binary_model (
    binary_model_id INT GENERATED ALWAYS AS IDENTITY,
    paper VARCHAR(200),
    candidate_name VARCHAR(50),
    eccentricity FLOAT4,
    m1 FLOAT4,
    m2 FLOAT4,
    mtot FLOAT4,
    mc FLOAT4,
    mu FLOAT4,
    q FLOAT4,
    evid1_type VARCHAR(100),
    evid1_note VARCHAR(500),
    evid1_wavelength VARCHAR(25),
    evid2_type VARCHAR(100),
    evid2_note VARCHAR(500),
    evid2_wavelength VARCHAR(25),
    evid3_type VARCHAR(100),
    evid3_note VARCHAR(500),
    evid3_wavelength VARCHAR(25),
    evid4_type VARCHAR(100),
    evid4_note VARCHAR(500),
    evid4_wavelength VARCHAR(25),
    inclination FLOAT4,
    semimajor_axis FLOAT4,
    seperation FLOAT4,
    period_epoch FLOAT4,
    orb_freq FLOAT8,
    orb_period FLOAT8,
    summary VARCHAR(500),
    caveats VARCHAR,
    ext_proj VARCHAR,
    PRIMARY KEY(binary_model_id),
    CONSTRAINT fk_candidate_name
        FOREIGN KEY(candidate_name)
            REFERENCES candidate(name)
);