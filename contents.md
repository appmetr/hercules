# Hercules – Flexible Apache Cassandra ORM for Java


## Introduction <a id="introduction"></a>

Hercules is a lightweight yet powerful ORM with an approach to provide OOP way of
interacting with Cassandra-specific data model.


### Features <a id="features"></a>

- plain entities (entity per row) and wide entities (entity by column in a row)
- flexible entity serialisation customisation
- managed entity indexes
- works great with DI frameworks
- lifecycle method/event support
- does not store null fields


### Get It! <a id="get-it"></a>

Current version is 0.4.2. Has been used only in several internal projects. *Use it in production at your own risk.*

Maven repo (currently hosted on BinTray – add this to `<repositories>` in your `pom.xml`):

	<repository>
		<id>appmetr-repo</id>
		<url>http://dl.bintray.com/appmetr/maven</url>
	</repository>


Maven dependency (add this to `<dependencies>` in your `pom.xml`):

	<dependency>
		<groupId>com.appmetr</groupId>
		<artifactId>hercules</artifactId>
		<version>0.4.2</version>
	</dependency>

https://bintray.com/appmetr/maven/hercules/0.4.2/files

Latest binaries and sources on BinTray: [hercules-0.4.2.jar](http://dl.bintray.com/appmetr/maven/com/appmetr/hercules/0.4.2/hercules-0.4.2.jar)

Latest source code: [hercules-0.4.2-sources.jar](http://dl.bintray.com/appmetr/maven/com/appmetr/hercules/0.4.2/hercules-0.4.2-sources.jar)

Fork on GitHub: [https://github.com/appmetr/hercules](https://github.com/appmetr/hercules)

Browse releases on BinTray: [https://bintray.com/appmetr/maven/hercules](https://bintray.com/appmetr/maven/hercules)

	


### Quick Start <a id="quick-start"></a>

Hercules requires Java SE 5 or higher.

Create you first entity:

	@Entity
	public class Cat {
		
		@Id String id;
		
		String name;
		
	}

	
Create entity DAO:

	public class CatDAO extends AbstractDAO<Cat, String> {

	    public CatDAO(Hercules hercules) {
	        super(Cat.class, hercules);
	    }

	}
	

Create Hercules config:

	Set<Class> entityClasses = new HashSet<Class>();
	entityClasses.add(Cat.class);

	HerculesConfig config = new HerculesConfig(
	    "Test", 			// keyspace name
	    "localhost:9160",	// cassandra host and port
	    10,					// max active connections
	    1,					// replication factor
	    true,				// is schema modification enabled
	    entityClasses
	);


Create Hercules and perform queries:

	Hercules hercules = HerculesFactory.create(config);
	hercules.init();
	
	CatDAO catDAO = new CatDAO(hercules);
	Cat cat = catDAO.get("cat01");
	
	hercules.shutdown();
	


## Reference

Coming soon...


## Team

Coming soon...


## Roadmap

Coming soon...
