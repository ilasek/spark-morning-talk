CREATE TABLE spark.public.patients (
  PatientId           BIGINT,
  AppointmentId       BIGINT,
  Gender              VARCHAR,
  ScheduledDay        TIMESTAMP,
  AppointmentDay      TIMESTAMP,
  Age                 INT,
  Neighbourhood       VARCHAR,
  Scholarship         INT,
  Hipertension        BOOLEAN,
  Diabetes            BOOLEAN,
  Alcoholism          INT,
  Handcap             BOOLEAN,
  SMS_received        INT,
  No_show             VARCHAR,
  Sick                BOOLEAN,
  PRIMARY KEY(PatientId)
);