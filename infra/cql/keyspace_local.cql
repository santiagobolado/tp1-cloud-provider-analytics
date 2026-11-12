-- Solo para Cassandra local (plan alternativo de la demo, riesgo G-02).
-- En AstraDB el keyspace se crea desde la consola.
CREATE KEYSPACE IF NOT EXISTS cloud_analytics
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
