-- Creation de la base de donnees 
CREATE DATABASE paludisme_senegal;
USE paludisme_senegal;

/* ⚠️ Note : La création de l'utilisateur et l'attribution des rôles sont
 effectuées séparément en environnement local. Voir documentation README.md
 pour la configuration */

-- Creation des tables de dimensions

-- dim_date
CREATE TABLE dim_date (
    id_date INT NOT NULL PRIMARY KEY,
    annee INT NOT NULL,
    trimestre INT NOT NULL,
    saison VARCHAR(30) NOT NULL
);

-- dim_region
CREATE TABLE dim_region (
    id_region   INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    nom_region  VARCHAR(100) NOT NULL,
    zone_geo    VARCHAR(50),
    pays        VARCHAR(50) DEFAULT 'Sénégal',
    CONSTRAINT UQ_nom_region UNIQUE (nom_region)
);

-- dim_indicateur 
CREATE TABLE dim_indicateur (
    id_indicateur INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    code_indicateur VARCHAR(100) NOT NULL,
    nom_indicateur VARCHAR(500) NOT NULL,
    categorie VARCHAR(100),
    unite VARCHAR(100),
    CONSTRAINT UQ_code_indicateur UNIQUE (code_indicateur)
);

-- dim_source
CREATE TABLE dim_source (
    id_source INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    nom_source VARCHAR(100) NOT NULL, 
    organisme VARCHAR(200) NOT NULL,          
    url VARCHAR(500),
    CONSTRAINT UQ_nom_source UNIQUE (nom_source)
);


-- dim_structure_sanitaire 
CREATE TABLE dim_structure_sanitaire (
    id_structure INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    nom_structure VARCHAR(255),
    type_structure VARCHAR(100),
    gestion VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    pays VARCHAR(50) DEFAULT 'Sénégal',
    id_region INT NOT NULL,
    CONSTRAINT fk_structure_region
        FOREIGN KEY (id_region) REFERENCES dim_region(id_region)
);

-- Creation de la table de fait

-- fact_cas_paludisme 
CREATE TABLE fact_paludisme (
    id_fait INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
    id_date INT NOT NULL,
    id_indicateur INT NOT NULL,
    id_source INT NOT NULL,
    id_region INT NOT NULL,
    valeur FLOAT,
    valeur_basse FLOAT,
    valeur_haute FLOAT,
    population_enquete FLOAT,

    CONSTRAINT fk_fait_date
        FOREIGN KEY (id_date) REFERENCES dim_date(id_date),
    CONSTRAINT fk_fait_indicateur
        FOREIGN KEY (id_indicateur) REFERENCES dim_indicateur(id_indicateur),
    CONSTRAINT fk_fait_source
        FOREIGN KEY (id_source) REFERENCES dim_source(id_source),
    CONSTRAINT fk_fait_region
        FOREIGN KEY (id_region) REFERENCES dim_region(id_region)
);

-- Index
CREATE NONCLUSTERED INDEX IX_fact_id_date
    ON fact_paludisme(id_date);

CREATE NONCLUSTERED INDEX IX_fact_id_region
    ON fact_paludisme(id_region);

CREATE NONCLUSTERED INDEX IX_fact_id_indicateur
    ON fact_paludisme(id_indicateur);

CREATE NONCLUSTERED INDEX IX_fact_id_source
    ON fact_paludisme(id_source);

