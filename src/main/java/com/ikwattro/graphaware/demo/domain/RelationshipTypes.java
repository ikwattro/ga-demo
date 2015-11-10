package com.ikwattro.graphaware.demo.domain;

import org.neo4j.graphdb.RelationshipType;

public enum RelationshipTypes implements RelationshipType{
    CONNECTS,
    KNOWS,
    MIGHT_NOW
}
