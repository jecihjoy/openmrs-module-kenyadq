/**
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */

package org.openmrs.module.kenyadq.api;

import org.openmrs.Patient;
import org.openmrs.api.APIException;
import org.openmrs.api.OpenmrsService;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

/**
 * Module service interface
 */
@Transactional
public interface KenyaDqService extends OpenmrsService {

	/**
	 * Merges the two given patients. The method applies some extra logic to the merge process in core to avoid leaving
	 * a patient with multiple identifiers of the same type.
	 * @param preferred the preferred patient
	 * @param notPreferred the non-preferred patient
	 */
	void mergePatients(Patient preferred, Patient notPreferred) throws APIException;

	List<Object> executeSqlQuery(String query, Map<String, Object> substitutions);

	List<Object> executeHqlQuery(String query, Map<String, Object> substitutions);
}