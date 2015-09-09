/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package backtype.storm.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-9-9")
public class ClusterSummary implements org.apache.thrift.TBase<ClusterSummary, ClusterSummary._Fields>, java.io.Serializable, Cloneable, Comparable<ClusterSummary> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ClusterSummary");

  private static final org.apache.thrift.protocol.TField SUPERVISORS_FIELD_DESC = new org.apache.thrift.protocol.TField("supervisors", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField TOPOLOGIES_FIELD_DESC = new org.apache.thrift.protocol.TField("topologies", org.apache.thrift.protocol.TType.LIST, (short)3);
  private static final org.apache.thrift.protocol.TField NIMBUSES_FIELD_DESC = new org.apache.thrift.protocol.TField("nimbuses", org.apache.thrift.protocol.TType.LIST, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ClusterSummaryStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ClusterSummaryTupleSchemeFactory());
  }

  private List<SupervisorSummary> supervisors; // required
  private List<TopologySummary> topologies; // required
  private List<NimbusSummary> nimbuses; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SUPERVISORS((short)1, "supervisors"),
    TOPOLOGIES((short)3, "topologies"),
    NIMBUSES((short)4, "nimbuses");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // SUPERVISORS
          return SUPERVISORS;
        case 3: // TOPOLOGIES
          return TOPOLOGIES;
        case 4: // NIMBUSES
          return NIMBUSES;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SUPERVISORS, new org.apache.thrift.meta_data.FieldMetaData("supervisors", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, SupervisorSummary.class))));
    tmpMap.put(_Fields.TOPOLOGIES, new org.apache.thrift.meta_data.FieldMetaData("topologies", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TopologySummary.class))));
    tmpMap.put(_Fields.NIMBUSES, new org.apache.thrift.meta_data.FieldMetaData("nimbuses", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NimbusSummary.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ClusterSummary.class, metaDataMap);
  }

  public ClusterSummary() {
  }

  public ClusterSummary(
    List<SupervisorSummary> supervisors,
    List<TopologySummary> topologies,
    List<NimbusSummary> nimbuses)
  {
    this();
    this.supervisors = supervisors;
    this.topologies = topologies;
    this.nimbuses = nimbuses;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ClusterSummary(ClusterSummary other) {
    if (other.is_set_supervisors()) {
      List<SupervisorSummary> __this__supervisors = new ArrayList<SupervisorSummary>(other.supervisors.size());
      for (SupervisorSummary other_element : other.supervisors) {
        __this__supervisors.add(new SupervisorSummary(other_element));
      }
      this.supervisors = __this__supervisors;
    }
    if (other.is_set_topologies()) {
      List<TopologySummary> __this__topologies = new ArrayList<TopologySummary>(other.topologies.size());
      for (TopologySummary other_element : other.topologies) {
        __this__topologies.add(new TopologySummary(other_element));
      }
      this.topologies = __this__topologies;
    }
    if (other.is_set_nimbuses()) {
      List<NimbusSummary> __this__nimbuses = new ArrayList<NimbusSummary>(other.nimbuses.size());
      for (NimbusSummary other_element : other.nimbuses) {
        __this__nimbuses.add(new NimbusSummary(other_element));
      }
      this.nimbuses = __this__nimbuses;
    }
  }

  public ClusterSummary deepCopy() {
    return new ClusterSummary(this);
  }

  @Override
  public void clear() {
    this.supervisors = null;
    this.topologies = null;
    this.nimbuses = null;
  }

  public int get_supervisors_size() {
    return (this.supervisors == null) ? 0 : this.supervisors.size();
  }

  public java.util.Iterator<SupervisorSummary> get_supervisors_iterator() {
    return (this.supervisors == null) ? null : this.supervisors.iterator();
  }

  public void add_to_supervisors(SupervisorSummary elem) {
    if (this.supervisors == null) {
      this.supervisors = new ArrayList<SupervisorSummary>();
    }
    this.supervisors.add(elem);
  }

  public List<SupervisorSummary> get_supervisors() {
    return this.supervisors;
  }

  public void set_supervisors(List<SupervisorSummary> supervisors) {
    this.supervisors = supervisors;
  }

  public void unset_supervisors() {
    this.supervisors = null;
  }

  /** Returns true if field supervisors is set (has been assigned a value) and false otherwise */
  public boolean is_set_supervisors() {
    return this.supervisors != null;
  }

  public void set_supervisors_isSet(boolean value) {
    if (!value) {
      this.supervisors = null;
    }
  }

  public int get_topologies_size() {
    return (this.topologies == null) ? 0 : this.topologies.size();
  }

  public java.util.Iterator<TopologySummary> get_topologies_iterator() {
    return (this.topologies == null) ? null : this.topologies.iterator();
  }

  public void add_to_topologies(TopologySummary elem) {
    if (this.topologies == null) {
      this.topologies = new ArrayList<TopologySummary>();
    }
    this.topologies.add(elem);
  }

  public List<TopologySummary> get_topologies() {
    return this.topologies;
  }

  public void set_topologies(List<TopologySummary> topologies) {
    this.topologies = topologies;
  }

  public void unset_topologies() {
    this.topologies = null;
  }

  /** Returns true if field topologies is set (has been assigned a value) and false otherwise */
  public boolean is_set_topologies() {
    return this.topologies != null;
  }

  public void set_topologies_isSet(boolean value) {
    if (!value) {
      this.topologies = null;
    }
  }

  public int get_nimbuses_size() {
    return (this.nimbuses == null) ? 0 : this.nimbuses.size();
  }

  public java.util.Iterator<NimbusSummary> get_nimbuses_iterator() {
    return (this.nimbuses == null) ? null : this.nimbuses.iterator();
  }

  public void add_to_nimbuses(NimbusSummary elem) {
    if (this.nimbuses == null) {
      this.nimbuses = new ArrayList<NimbusSummary>();
    }
    this.nimbuses.add(elem);
  }

  public List<NimbusSummary> get_nimbuses() {
    return this.nimbuses;
  }

  public void set_nimbuses(List<NimbusSummary> nimbuses) {
    this.nimbuses = nimbuses;
  }

  public void unset_nimbuses() {
    this.nimbuses = null;
  }

  /** Returns true if field nimbuses is set (has been assigned a value) and false otherwise */
  public boolean is_set_nimbuses() {
    return this.nimbuses != null;
  }

  public void set_nimbuses_isSet(boolean value) {
    if (!value) {
      this.nimbuses = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SUPERVISORS:
      if (value == null) {
        unset_supervisors();
      } else {
        set_supervisors((List<SupervisorSummary>)value);
      }
      break;

    case TOPOLOGIES:
      if (value == null) {
        unset_topologies();
      } else {
        set_topologies((List<TopologySummary>)value);
      }
      break;

    case NIMBUSES:
      if (value == null) {
        unset_nimbuses();
      } else {
        set_nimbuses((List<NimbusSummary>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SUPERVISORS:
      return get_supervisors();

    case TOPOLOGIES:
      return get_topologies();

    case NIMBUSES:
      return get_nimbuses();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SUPERVISORS:
      return is_set_supervisors();
    case TOPOLOGIES:
      return is_set_topologies();
    case NIMBUSES:
      return is_set_nimbuses();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ClusterSummary)
      return this.equals((ClusterSummary)that);
    return false;
  }

  public boolean equals(ClusterSummary that) {
    if (that == null)
      return false;

    boolean this_present_supervisors = true && this.is_set_supervisors();
    boolean that_present_supervisors = true && that.is_set_supervisors();
    if (this_present_supervisors || that_present_supervisors) {
      if (!(this_present_supervisors && that_present_supervisors))
        return false;
      if (!this.supervisors.equals(that.supervisors))
        return false;
    }

    boolean this_present_topologies = true && this.is_set_topologies();
    boolean that_present_topologies = true && that.is_set_topologies();
    if (this_present_topologies || that_present_topologies) {
      if (!(this_present_topologies && that_present_topologies))
        return false;
      if (!this.topologies.equals(that.topologies))
        return false;
    }

    boolean this_present_nimbuses = true && this.is_set_nimbuses();
    boolean that_present_nimbuses = true && that.is_set_nimbuses();
    if (this_present_nimbuses || that_present_nimbuses) {
      if (!(this_present_nimbuses && that_present_nimbuses))
        return false;
      if (!this.nimbuses.equals(that.nimbuses))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_supervisors = true && (is_set_supervisors());
    list.add(present_supervisors);
    if (present_supervisors)
      list.add(supervisors);

    boolean present_topologies = true && (is_set_topologies());
    list.add(present_topologies);
    if (present_topologies)
      list.add(topologies);

    boolean present_nimbuses = true && (is_set_nimbuses());
    list.add(present_nimbuses);
    if (present_nimbuses)
      list.add(nimbuses);

    return list.hashCode();
  }

  @Override
  public int compareTo(ClusterSummary other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_supervisors()).compareTo(other.is_set_supervisors());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_supervisors()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.supervisors, other.supervisors);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_topologies()).compareTo(other.is_set_topologies());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_topologies()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.topologies, other.topologies);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_nimbuses()).compareTo(other.is_set_nimbuses());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_nimbuses()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.nimbuses, other.nimbuses);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ClusterSummary(");
    boolean first = true;

    sb.append("supervisors:");
    if (this.supervisors == null) {
      sb.append("null");
    } else {
      sb.append(this.supervisors);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("topologies:");
    if (this.topologies == null) {
      sb.append("null");
    } else {
      sb.append(this.topologies);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("nimbuses:");
    if (this.nimbuses == null) {
      sb.append("null");
    } else {
      sb.append(this.nimbuses);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_supervisors()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'supervisors' is unset! Struct:" + toString());
    }

    if (!is_set_topologies()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'topologies' is unset! Struct:" + toString());
    }

    if (!is_set_nimbuses()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'nimbuses' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class ClusterSummaryStandardSchemeFactory implements SchemeFactory {
    public ClusterSummaryStandardScheme getScheme() {
      return new ClusterSummaryStandardScheme();
    }
  }

  private static class ClusterSummaryStandardScheme extends StandardScheme<ClusterSummary> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ClusterSummary struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SUPERVISORS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list74 = iprot.readListBegin();
                struct.supervisors = new ArrayList<SupervisorSummary>(_list74.size);
                SupervisorSummary _elem75;
                for (int _i76 = 0; _i76 < _list74.size; ++_i76)
                {
                  _elem75 = new SupervisorSummary();
                  _elem75.read(iprot);
                  struct.supervisors.add(_elem75);
                }
                iprot.readListEnd();
              }
              struct.set_supervisors_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // TOPOLOGIES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list77 = iprot.readListBegin();
                struct.topologies = new ArrayList<TopologySummary>(_list77.size);
                TopologySummary _elem78;
                for (int _i79 = 0; _i79 < _list77.size; ++_i79)
                {
                  _elem78 = new TopologySummary();
                  _elem78.read(iprot);
                  struct.topologies.add(_elem78);
                }
                iprot.readListEnd();
              }
              struct.set_topologies_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // NIMBUSES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list80 = iprot.readListBegin();
                struct.nimbuses = new ArrayList<NimbusSummary>(_list80.size);
                NimbusSummary _elem81;
                for (int _i82 = 0; _i82 < _list80.size; ++_i82)
                {
                  _elem81 = new NimbusSummary();
                  _elem81.read(iprot);
                  struct.nimbuses.add(_elem81);
                }
                iprot.readListEnd();
              }
              struct.set_nimbuses_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, ClusterSummary struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.supervisors != null) {
        oprot.writeFieldBegin(SUPERVISORS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.supervisors.size()));
          for (SupervisorSummary _iter83 : struct.supervisors)
          {
            _iter83.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.topologies != null) {
        oprot.writeFieldBegin(TOPOLOGIES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.topologies.size()));
          for (TopologySummary _iter84 : struct.topologies)
          {
            _iter84.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.nimbuses != null) {
        oprot.writeFieldBegin(NIMBUSES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.nimbuses.size()));
          for (NimbusSummary _iter85 : struct.nimbuses)
          {
            _iter85.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ClusterSummaryTupleSchemeFactory implements SchemeFactory {
    public ClusterSummaryTupleScheme getScheme() {
      return new ClusterSummaryTupleScheme();
    }
  }

  private static class ClusterSummaryTupleScheme extends TupleScheme<ClusterSummary> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ClusterSummary struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.supervisors.size());
        for (SupervisorSummary _iter86 : struct.supervisors)
        {
          _iter86.write(oprot);
        }
      }
      {
        oprot.writeI32(struct.topologies.size());
        for (TopologySummary _iter87 : struct.topologies)
        {
          _iter87.write(oprot);
        }
      }
      {
        oprot.writeI32(struct.nimbuses.size());
        for (NimbusSummary _iter88 : struct.nimbuses)
        {
          _iter88.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ClusterSummary struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list89 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.supervisors = new ArrayList<SupervisorSummary>(_list89.size);
        SupervisorSummary _elem90;
        for (int _i91 = 0; _i91 < _list89.size; ++_i91)
        {
          _elem90 = new SupervisorSummary();
          _elem90.read(iprot);
          struct.supervisors.add(_elem90);
        }
      }
      struct.set_supervisors_isSet(true);
      {
        org.apache.thrift.protocol.TList _list92 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.topologies = new ArrayList<TopologySummary>(_list92.size);
        TopologySummary _elem93;
        for (int _i94 = 0; _i94 < _list92.size; ++_i94)
        {
          _elem93 = new TopologySummary();
          _elem93.read(iprot);
          struct.topologies.add(_elem93);
        }
      }
      struct.set_topologies_isSet(true);
      {
        org.apache.thrift.protocol.TList _list95 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.nimbuses = new ArrayList<NimbusSummary>(_list95.size);
        NimbusSummary _elem96;
        for (int _i97 = 0; _i97 < _list95.size; ++_i97)
        {
          _elem96 = new NimbusSummary();
          _elem96.read(iprot);
          struct.nimbuses.add(_elem96);
        }
      }
      struct.set_nimbuses_isSet(true);
    }
  }

}

