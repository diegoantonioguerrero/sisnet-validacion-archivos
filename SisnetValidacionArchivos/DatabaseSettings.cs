using System.Collections.Generic;
using System.Configuration;

namespace SisnetValidacionArchivos
{
    public class DatabaseSettings : ConfigurationSection
    {
        [ConfigurationProperty("databases", IsDefaultCollection = false)]
        [ConfigurationCollection(typeof(DatabaseCollection), AddItemName = "database")]
        public DatabaseCollection Databases
        {
            get { return (DatabaseCollection)this["databases"]; }
        }
    }

    public class DatabaseCollection : ConfigurationElementCollection, IEnumerable<DatabaseElement>
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new DatabaseElement();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((DatabaseElement)element).Database;
        }

        public new IEnumerator<DatabaseElement> GetEnumerator()
        {
            foreach (var key in BaseGetAllKeys())
            {
                yield return (DatabaseElement)BaseGet(key);
            }
        }
    }

    public class DatabaseElement : ConfigurationElement
    {
        [ConfigurationProperty("server", IsRequired = true)]
        public string Server
        {
            get { return (string)this["server"]; }
            set { this["server"] = value; }
        }

        [ConfigurationProperty("database", IsRequired = true)]
        public string Database
        {
            get { return (string)this["database"]; }
            set { this["database"] = value; }
        }

        [ConfigurationProperty("port", IsRequired = true)]
        public string Port
        {
            get { return (string)this["port"]; }
            set { this["port"] = value; }
        }

        [ConfigurationProperty("user", IsRequired = true)]
        public string User
        {
            get { return (string)this["user"]; }
            set { this["user"] = value; }
        }

        [ConfigurationProperty("password", IsRequired = true)]
        public string Password
        {
            get { return (string)this["password"]; }
            set { this["password"] = value; }
        }

        [ConfigurationProperty("tableToValidate", IsRequired = true)]
        public string TableToValidate
        {
            get { return (string)this["tableToValidate"]; }
            set { this["tableToValidate"] = value; }
        }
    }
}