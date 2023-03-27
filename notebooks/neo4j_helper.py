from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

class App:

    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
    
    def close(self):
        self.driver.close()

    def __str__(self) -> str:
        return f"App(uri={self.driver.uri}, database={self.database})"

    
    def in_organism(self, protien_id, organism, taxonomy_id):
        with self.driver.session(database=self.database) as session:
            result = session.execute_write(
                self._in_organism, protien_id, organism, taxonomy_id
            )
            for row in result:
                print(f"Created relationship where ({row['p']}) -> ({row['o']})")

    @staticmethod
    def _in_organism(tx, protein_id, organism_name, taxonomy_id):
        """
        Protein (id) -[in_organism]-> Organism(name, taxonomy_id)
        """
        query = (
            " CREATE (p:Protein { id: $protein_id }) "
            " CREATE (o:Organism { name: $organism_name, taxonomy_id: $taxonomy_id }) "
            " (p)-[:FROM]->(o) "
            " RETURN p, o "
        )
        result = tx.run(query, protein_id=protein_id, organism_name=organism_name, taxonomy_id=taxonomy_id )
        try:
            return [{"p": row["p"]["id"], "o_name": row["o"]["organism_name"], "o_tax_id":row["o"]["taxonomy_id"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise



    def has_full_name(self, protien_id, full_name):
        with self.driver.session(database=self.database) as session:
            result = session.execute_write(
                self._has_full_name, protien_id, full_name
            )
            for row in result:
                print(f"Created relationship where ({row['p']}) -> ({row['fn']})")



    @staticmethod
    def _has_full_name(tx, protein_id, full_name):
        """
        Protein (id) -[has_full_name]-> FullName(name)
        """
        query = (
            "CREATE (p:Protein { id: $protein_id }) "
            "CREATE (fn:FullName { name: $full_name }) "
            "CREATE (p)-[:HAS_FULL_NAME]->(fn) "
            "RETURN p, fn"
        )
        result = tx.run(query, protein_id=protein_id, full_name=full_name)
        try:
            return [{"p": row["p"]["id"], "fn": row["fn"]["name"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise




    def has_reference(self, reference_id, reference_type, reference_name, author_name):
        with self.driver.session(database=self.database) as session:
            result = session.execute_write(
                self._has_reference, reference_id, reference_type, reference_name, author_name
            )
            for row in result:
                print(f"Created relationship where ({row['p']}) -> ({row['r']}) -> ({row['a']})")


    @staticmethod
    def _has_reference(tx, protein_id, reference_id, reference_type, reference_name, author_name):
        """
        Protein(id) -[has_reference]-> Reference(id, type, name) -[has_author]-> Author(name)
        """
        query = (
            "CREATE (p:Protein { id: $protein_id }) "
            "CREATE (r:Reference { id: $reference_id, type: $reference_type, name: $reference_name }) "
            "CREATE (a:Author { name: $author_name }) "
            "CREATE (r)-[:HAS_AUTHOR]->(a) "
            "CREATE (p)-[:HAS_REFERENCE]->(r) "
            "RETURN p, r, a"
        )
        result = tx.run(query, protein_id=protein_id, reference_id=reference_id, reference_type=reference_type,
                        reference_name=reference_name, author_name=author_name)
        try:
            return [{
                "p": row["p"]["id"], 
                "r_id": row["r"]["id"], 
                "r_type": row["r"]["type"], 
                "r_name": row["r"]["name"],
                "a_name": row["a"]["name"]
            } for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
    



    def has_feature(self, protein_id, feature_name, feature_type):
        with self.driver.session(database=self.database) as session:
            result = session.execute_write(
                self._has_feature, protein_id, feature_name, feature_type
            )
            for row in result:
                print(f"Created relationship where ({row['p']}) -> ({row['f']})")



    @staticmethod
    def _has_feature(tx, protein_id, feature_name, feature_type):
        """
        Protein(id) -[has_feature]-> feature(name, type)
        """
        query = (
            "CREATE (p:Protein { id: $protein_id }) "
            "CREATE (f:Feature { name: $feature_name, type: $feature_type }) "
            "CREATE (p)-[:HAS_FEATURE]->(f) "
            "RETURN p, f"
        )
        result = tx.run(query, protein_id=protein_id, feature_name=feature_name, feature_type=feature_type)
        try:
            return [{
                "p": row["p"]["id"], 
                "f_name": row["f"]["name"], 
                "f_type": row["f"]["type"]
                }
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise





    def from_gene_primary(self, protein_id, gene_name):
        with self.driver.session(database=self.database) as session:
            result = session.execute_write(
                self._from_gene_primary, protein_id, gene_name
            )
            for row in result:
                print(f"Created relationship where ({row['p']}) -> ({row['g']})")



    @staticmethod
    def _from_gene_primary(tx, protein_id, gene_name):
        """
        Protein(id) -[from_gene_primary]-> Gene(name)
        """
        query = (
            "CREATE (p:Protein { id: $protein_id }) "
            "CREATE (g:Gene { name: $gene_name }) "
            "CREATE (p)-[:FROM_GENE_PRIMARY]->(g) "
            "RETURN p, g"
        )
        result = tx.run(query, protein_id=protein_id, gene_name=gene_name)
        try:
            return [{"p": row["p"]["id"], "g_name": row["g"]["name"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise




    def from_gene_synonym(self, protein_id, gene_name):
        with self.driver.session(database=self.database) as session:
            result = session.execute_write(
                self._from_gene_synonym, protein_id, gene_name
            )
            for row in result:
                print(f"Created relationship where ({row['p']}) -> ({row['g']})")


    @staticmethod
    def _from_gene_synonym(tx, protein_id, gene_name):
        """
        Protein(id) -[from_gene_synonym]-> Gene(name)
        """
        query = (
            "CREATE (p:Protein { id: $protein_id }) "
            "MATCH (g:Gene { name: $gene_name }) "
            "CREATE (p)-[:FROM_GENE_SYNONYM]->(g) "
            "RETURN p, g"
        )
        result = tx.run(query, protein_id=protein_id, gene_name=gene_name)
        try:
            return [{"p": row["p"]["id"], "g_name": row["g"]["name"]}
                    for row in result]
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
