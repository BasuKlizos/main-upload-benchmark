import json
import re
from collections import Counter
from typing import Any, Dict, Tuple

from backend.config import settings
from backend.db_config.db import collection
from backend.logging_config.logger import logger
from backend.misc import Toolkit
from backend.schemas import candidate, jobs
from backend.service.openai_client import _create_chat_completion
from bson import ObjectId
from jsonschema import ValidationError, validate
from textstat import flesch_kincaid_grade


class _AIParser(object):
    """Base class for AI parsing functionality"""

    def __init__(self):
        self.users = collection("users")

    def update_token_usage_per_user(self, user_id: str, openai_model: str, response: object):
        input_tokens = response.usage.prompt_tokens if response.usage else 0
        output_tokens = response.usage.completion_tokens if response.usage else 0

        self.users.update_one(
            {"_id": ObjectId(user_id)},
            {"$inc": {f"token_usage.{openai_model}.input_token": input_tokens, f"token_usage.{openai_model}.output_token": output_tokens}},
            upsert=True,  # Ensures the document is created if it doesn’t exist
        )

    def _clean_response(self, content: str) -> str:
        """Clean API response by removing markdown and extra formatting"""
        content = content.strip()
        if content.startswith("```json"):
            content = content[7:].strip()
        elif content.startswith("```"):
            content = content[3:].strip()
        if content.endswith("```"):
            content = content[:-3].strip()
        return content


class _CVParserClient(_AIParser):
    """Handles communication with OpenAI API for CV parsing"""

    def __init__(self, is_image: bool = False):
        super().__init__()
        self.is_image = is_image
        self.prompt_template = self._get_prompt_template(self.is_image)

    def _get_prompt_template(self, is_image) -> str:
        return rf"""
        You are an AI resume parser. The CV {'text' if is_image else 'images'} provided may span multiple pages. Make sure to merge any fragmented data, **Translate non-English job-related terms (e.g., skills, responsibilities, job titles, certifications) while preserving proper nouns (e.g., company and place names), ensuring precision and natural fluency.**, and return a strictly valid JSON (no extra keys or commentary) matching the structure below. 
        If a field is missing or cannot be determined, use "" (for strings), 0 (for numbers), or [] (for arrays).

        **Key Instructions**:
        - **Name**: Format in Title Case; remove special characters (dots, dashes, etc.) and fix spacing.
        - **Contact**: 
          * "primary_phone" = first phone, "secondary_phone" = second.
          * "email" = first email, "secondary_email" = second.
        - **Location**: Extract only if explicitly stated (do not infer from job locations).
        - **Work Experience**:
          * For multiple roles at a single company, create separate entries.
          * Indicate if the role was remote, in-office, or hybrid.
          * Format duration as "X years Y months" (or "Y months" if <1 year).
          * Provide a concise "responsibility_summary" (1–2 sentences) for each entry.
        - **Total Experience**: Sum durations from all work experiences (same format as above).
        - **Social Profiles**: Extract URLs (e.g., GitHub, LinkedIn, Twitter, LeetCode, others).
        - **Education**: Include degree, institution, graduation year, and any score/percentage/CGPA if available.
        - **Personal Details**: Include date of birth, age, gender, and languages (with proficiency levels) if provided.
        - **Certifications**:
          * Each must be an object with "name", "issuer", "issue_date", "expiry_date", and "verification_link".
          * If only a name is found, fill the remaining fields with "" (never just a string).
        - **Projects**:
          * Extract "name", "project_type" (personal/professional/academic), "description", and "technologies" (as a list).
          * If technologies aren’t explicitly mentioned, infer them from the context.
        - **Additional Sections**: Include hobbies/interests, strengths/weaknesses, and current role/designation if available.
        - **Format**: Use double-quoted keys, no trailing commas or extra commentary.

        **JSON Structure**:
        {{
            "name": "",
            "email": "",
            "secondary_email": "",
            "primary_phone": "",
            "secondary_phone": "",
            "location": "",
            "designation": "",
            "personal_details": {{
                "date_of_birth": "",
                "age": 0,
                "gender": ""
            }},
            "social_profiles": {{
                "github": "",
                "linkedin": "",
                "twitter": "",
                "leetcode": "",
                "others": ["include all found"]
            }},
            "education": [
                {{
                    "degree": "",
                    "institution": "",
                    "graduation_year": 0,
                    "score": ""
                }}
            ],
            "work_experience": [
                {{
                    "company_name": "",
                    "position": "",
                    "duration": "",
                    "is_remote": false,
                    "responsibilities": ["include all found"],
                    "responsibility_summary": ""
                }}
            ],
            "total_experience": "",
            "technical_skills": ["include all found"],
            "soft_skills": ["include all found"],
            "languages": [
                {{
                    "name": "",
                    "proficiency": ""
                }}
            ],
            "certifications": [
                {{
                    "name": "",
                    "issuer": "",
                    "issue_date": "",
                    "expiry_date": "",
                    "verification_link": ""
                }}
            ],
            "projects": [
                {{
                    "name": "",
                    "project_type": "",
                    "description": "",
                    "technologies": ["include all found"]
                }}
            ],
            "strengths": [""],
            "weaknesses": [""],
            "hobbies_interests": [""]
        }}

        **CV Text**:
        <<Insert the full CV text here>>
    """

    async def extract_heuristics(self, text: str) -> Dict[str, Any]:
        """Extract text analysis metrics from CV text.

        Analyzes the given text to extract various metrics including word count,
        personal pronoun usage, reading level, vocabulary diversity, and most frequent words.

        Args:
            text: The CV text to analyze

        Returns:
            Dict containing the following metrics:
                - word_count: Total number of words
                - personal_pronoun_count: Number of personal pronouns used
                - reading_level: Flesch-Kincaid grade level score
                - vocabulary_level: Vocabulary diversity score (unique words / total words * 10)
                - frequent_words: List of 10 most frequently used words
        """
        try:
            word_count = len(text.split())
            pronouns = len(re.findall(r"\b(I|me|my|mine)\b", text, re.IGNORECASE))
            frequent_words = [word for word, _ in Counter(text.lower().split()).most_common(10)]
            return {
                "word_count": word_count,
                "personal_pronoun_count": pronouns,
                "reading_level": round(flesch_kincaid_grade(text), 2),
                "vocabulary_level": round(len(set(text.lower().split())) / word_count * 10, 2),
                "frequent_words": frequent_words,
            }
        except Exception as e:
            logger.error(f"Error extracting heuristics: {str(e)}")
            raise

    async def parse_text(self, cv_text: str, user_id: str) -> candidate.CVParseResponse:
        """Parse CV text using OpenAI API and extract structured data.

        Args:
            cv_text: Raw text content of the CV to parse
            user_id: ID of the user making the request

        Returns:
            CVParseResponse: Structured CV data

        Raises:
            ValueError: If parsing fails or invalid JSON is received
        """
        try:
            # Call OpenAI API to parse CV text
            response = await _create_chat_completion(
                model=settings.OPENAI_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a professional CV parsing assistant. Return only valid, properly formatted JSON that matches the specified structure exactly. Ensure all property names are in double quotes. For certifications, return only the certification name as a string, not an object.",
                    },
                    {
                        "role": "user",
                        "content": f"{self.prompt_template}\n\nCV Text:\n{cv_text}",
                    },
                ],
                temperature=0.2,
                max_tokens=4000,
                timeout=60,
            )

            # Track token usage for billing
            self.update_token_usage_per_user(user_id, settings.OPENAI_MODEL, response)
            parsed_data = response.choices[0].message.content.strip()
            # Validate JSON before processing
            try:
                # Validate and convert to CVParseResponse model
                parsed_json = json.loads(parsed_data)
                json_str = json.dumps(parsed_json)
                return candidate.CVParseResponse.model_validate_json(json_str)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON received from OpenAI: {parsed_data}")
                raise ValueError(f"Failed to parse CV - Invalid JSON format: {str(e)}")

        except Exception as e:
            logger.error(f"Error parsing CV: {str(e)}")
            raise ValueError(f"Failed to parse CV: {str(e)}")


class _JobDescriptionParserClient(_AIParser):
    """Client for parsing job descriptions using OpenAI API"""

    def __init__(self):
        super().__init__()

    async def parse_job_description(self, job_text: str, user_id: str, job_title: str) -> jobs.JobDescription:
        """
        Parse job description text into structured data.

        Args:
            job_text: Raw job description text

        Returns:
            JobDescription: Structured job description data

        Raises:
            Exception: If parsing fails
        """
        prompt = """
            Parse this job description into JSON with these fields:
            - address: Overall Job location
            - location: Extract the City, Country from the address and put it as (City, Country)
            - job_post_name: Take the title of the job with location and experience and make it in a Job Post
            - role_description: Role summary
            - experience: Experience required for the job like (1-2 years, 2-3 years, etc.)
            - responsibilities: List of key duties
            - requirements: List of required skills/qualifications
            - must_haves: List of mandatory requirements
            - nice_to_have: List of preferred qualifications
            - preferred_skills: List of preferred/required skills for the job
            - job_mode: Mode of the job (remote, hybrid, in-office)
            - employment_type: Type of employment (full-time, part-time, contract, internship)

            Return only valid JSON for the job description below:

            {job_text}
        """

        try:
            response = await _create_chat_completion(
                model=settings.OPENAI_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": f"You are an expert job description parser. You are given a job description and a job title: {job_title}",
                    },
                    {"role": "user", "content": prompt.format(job_text=job_text, job_title=job_title)},
                ],
                temperature=0.2,
                max_tokens=1500,
                timeout=60,
            )
            # Extract token usage
            self.update_token_usage_per_user(user_id, settings.OPENAI_MODEL, response)
            content = self._clean_response(response.choices[0].message.content)

            # Extract JSON object
            json_match = re.search(r"\{.*\}", content, re.DOTALL)
            if not json_match:
                raise ValueError("No valid JSON found in response")

            parsed_data = json.loads(json_match.group(0))
            return jobs.JobDescription.model_validate(parsed_data)

        except Exception as e:
            logger.error("Failed to parse job description: %s", str(e))
            raise Exception(f"Job description parsing failed: {str(e)}")

    async def generate_questions(self, job: dict, count: int, user_id: str) -> list[str]:
        """
        Generate questions for a job based on the job details.

        Args:
            job (dict): Job details including title, requirements etc.
            count (int): Number of questions to generate

        Returns:
            list[str]: List of generated interview questions
        """
        prompt = f"""
        You are an interviewer specialized in designing interview questions to help hiring managers find candidates with strong technical expertise and project experience.

        Job Title: {job.get('job_post_name', '')}
        Location: {job.get('location', '')}
        Role Description: {job.get('role_description', '')}
        Experience Required: {job.get('experience', '')}
        
        Key Requirements:
        - Responsibilities: {job.get('responsibilities', [])}
        - Requirements: {job.get('requirements', [])}
        - Must Have Skills: {job.get('must_haves', [])}
        - Nice to Have Skills: {job.get('nice_to_have', [])}
        - Preferred Skills: {job.get('preferred_skills', [])}
        - Job Mode: {job.get('job_mode', '')}

        Generate {count} interview questions and {count} questions only following the below guidelines:
        - Focus on evaluating the candidate's technical knowledge and their experience working on relevant projects. Questions should aim to gauge depth of expertise, problem-solving ability, and hands-on project experience. These aspects carry the most weight.
        - Include questions designed to assess problem-solving skills through practical examples. For instance, how the candidate has tackled challenges in previous projects, and their approach to complex technical issues.
        - Soft skills such as communication, teamwork, and adaptability should be addressed, but given less emphasis compared to technical and problem-solving abilities.
        - Maintain a professional yet approachable tone, ensuring candidates feel comfortable while demonstrating their knowledge.
        - Keep questions concise (30 words or less) but open-ended
        - Questions should help validate candidate fit for this specific role
        - Questions should cover both technical requirements and role responsibilities

        Return only a JSON array of questions.
        """

        try:
            response = await _create_chat_completion(
                model=settings.OPENAI_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at creating targeted interview questions.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=1500,
                timeout=60,
            )

            # Extract token usage
            self.update_token_usage_per_user(user_id, settings.OPENAI_MODEL, response)

            # Clean and parse response
            content = self._clean_response(response.choices[0].message.content)
            json_match = re.search(r"\[.*\]", content, re.DOTALL)

            if not json_match:
                raise ValueError("No valid question array found in response")

            questions = json.loads(json_match.group(0))

            if not isinstance(questions, list):
                raise ValueError("Response is not a list of questions")

            return questions

        except Exception as e:
            logger.error("Failed to generate interview questions: %s", str(e))
            raise Exception(f"Question generation failed: {str(e)}")


class _CandidateSpecificQuestionsClient(_AIParser):
    """Client for generating candidate-specific questions using OpenAI API"""

    def __init__(self):
        super().__init__()

    async def generate(self, job: dict, embedding_text: dict, user_id: str) -> list[str]:
        prompt = f"""
        You are an expert technical interviewer. Generate only 5 targeted interview questions based on the candidate's specific background and 
        the job requirements mostly based on the candidate's experience and skills.

        Focus areas:
        1. Technical depth in skills that match job requirements
        2. Relevant project experience and achievements
        3. Areas where candidate's experience aligns with or differs from job needs
        4. Leadership and soft skills demonstrated in past roles
        5. Growth trajectory and skill development

        Questions should:
        - Be specific to this candidate's background
        - Help validate fit for this role's requirements
        - Uncover depth of expertise in key technical areas
        - Explore relevant past projects and responsibilities
        - Avoid generic/basic questions
        - Each include a clear purpose/what you're evaluating
        - Make sure the questions are not generic and are specific to the candidate's background and experience.

        *Questions Must be Short and concise and to the point.

        Return the questions in an array of strings

        Job Details:
        Title: {job.get('job_post_name', '')}
        Description: {job.get('role_description', '')}
        Required Experience: {job.get('experience', '')}
        Mode: {job.get('job_mode', '')}

        Requirements:
        - Core Responsibilities: {job.get('responsibilities', [])}
        - Required Skills: {job.get('requirements', [])} 
        - Must Have: {job.get('must_haves', [])}
        - Nice to Have: {job.get('nice_to_have', [])}
        - Preferred: {job.get('preferred_skills', [])}

        Candidate Background:
        {embedding_text}
        """

        try:
            response = await _create_chat_completion(
                model=settings.OPENAI_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at creating targeted interview questions.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=1500,
                timeout=60,
            )
            # Extract token usage
            self.update_token_usage_per_user(user_id, settings.OPENAI_MODEL, response)

            # Clean and parse response
            content = self._clean_response(response.choices[0].message.content)
            json_match = re.search(r"\[.*\]", content, re.DOTALL)

            if not json_match:
                raise ValueError("No valid question array found in response")

            questions = json.loads(json_match.group(0))

            if not isinstance(questions, list):
                raise ValueError("Response is not a list of questions")

            return questions

        except Exception as e:
            logger.error("Failed to generate interview questions: %s", str(e))
            raise Exception(f"Question generation failed: {str(e)}")


class _CompatibilityAnalyzer(_AIParser):
    """Analyzes compatibility between job requirements and candidate profiles using AI."""

    def __init__(self):
        """Initialize the CompatibilityAnalyzer."""
        super().__init__()

    def _clean_response(self, text: str) -> str:
        """Clean and normalize the response text from OpenAI API.

        Args:
            text (str): Raw response text from OpenAI

        Returns:
            str: Cleaned and normalized text
        """
        return text.strip().replace("```json", "").replace("```", "")

    def _extract_details(self, candidate: dict) -> Tuple[str, str]:
        """Extract and format candidate details for analysis.

        Args:
            candidate (dict): Candidate profile data

        Returns:
            Tuple[str, str]: Formatted work experience and education
        """
        # Format work experience with more details
        work_experience = (
            "\n".join(
                [
                    f"• {exp.get('company_name', 'Unknown Company')}: {exp.get('position', 'Unknown Position')} "
                    f"({exp.get('duration', 'Duration not specified')})\n"
                    f"  Description: {exp.get('description', 'No description provided')}"
                    for exp in candidate.get("work_experience", [])
                ]
            )
            or "No work experience listed"
        )

        # Format education with more details
        education = (
            "\n".join(
                [
                    f"• {edu.get('degree', 'Unknown Degree')} - {edu.get('institution', 'Unknown Institution')} "
                    f"({edu.get('graduation_year', 'Year not specified')})\n"
                    f"  Major: {edu.get('major', 'Not specified')}"
                    for edu in candidate.get("education", [])
                ]
            )
            or "No education listed"
        )

        return work_experience, education

    def _get_prompt(self, job: dict, candidate: dict, work_experience: str, education: str) -> str:
        """Build the full LLM prompt with an embedded few-shot example for consistent outputs."""

        few_shot_example = """
            Example Response:
            {
            "overall_score": 82.5,
            "analysis": {
                "strengths": [
                "Strong Python development skills",
                "5 years of relevant backend experience",
                "Familiarity with microservices architecture"
                ],
                "gaps": [
                "No AWS certification",
                "Limited exposure to container orchestration"
                ],
                "matching_points": [
                "Proficient in Python",
                "Used Django and REST APIs",
                "Worked in Agile teams"
                ],
                "improvement_areas": [
                "Obtain AWS Solutions Architect Certification",
                "Gain experience with Kubernetes"
                ]
            },
            "detailed_scoring": {
                "skills_match": 85.0,
                "experience_relevance": 80.0,
                "education_fit": 75.0,
                "overall_potential": 90.0
            }
            }
            """

        prompt = f"""{few_shot_example}

            You are an expert AI recruiter. Analyze the following job description and candidate profile for compatibility.

            JOB DESCRIPTION

            ---------------
            Description:
            {job.get('description', 'Not provided')}

            Required Skills:
            {', '.join(job.get('required_skills', ['Not specified']))}

            Experience Required:
            {job.get('required_experience', 'Not specified')} years

            Education Required:
            {job.get('required_education', 'Not specified')}

            CANDIDATE PROFILE
            -----------------
            Name: {candidate.get('name', 'Not provided')}

            Technical Skills:
            {', '.join(candidate.get('technical_skills', ['None listed']))}

            Soft Skills:
            {', '.join(candidate.get('soft_skills', ['None listed']))}

            Work Experience:
            {work_experience}

            Education:
            {education}

            Certifications:
            {', '.join(cert.get('name', 'Unknown Certification') for cert in candidate.get('certifications', []))}

            ANALYSIS REQUIRED
            -----------------
            Return a structured JSON object with the following fields:
            - "overall_score": float between 0 and 100
            - "analysis": {{
                "strengths": [...],
                "gaps": [...],
                "matching_points": [...],
                "improvement_areas": [...]
            }}
            - "detailed_scoring": {{
                "skills_match": float,
                "experience_relevance": float,
                "education_fit": float,
                "overall_potential": float
            }}

            Only output the JSON response. Do not include explanations or comments.
        """

        return prompt

    def _validate_schema(self, analysis: Dict[str, Any]) -> None:
        schema = {
            "type": "object",
            "properties": {
                "overall_score": {"type": "number"},
                "analysis": {
                    "type": "object",
                    "properties": {
                        "strengths": {"type": "array", "items": {"type": "string"}},
                        "gaps": {"type": "array", "items": {"type": "string"}},
                        "matching_points": {"type": "array", "items": {"type": "string"}},
                        "improvement_areas": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["strengths", "gaps", "matching_points", "improvement_areas"],
                },
                "detailed_scoring": {
                    "type": "object",
                    "properties": {
                        "skills_match": {"type": "number"},
                        "experience_relevance": {"type": "number"},
                        "education_fit": {"type": "number"},
                        "overall_potential": {"type": "number"},
                    },
                    "required": ["skills_match", "experience_relevance", "education_fit", "overall_potential"],
                },
            },
            "required": ["overall_score", "analysis", "detailed_scoring"],
        }
        validate(instance=analysis, schema=schema)

    async def get_analysis(self, job: dict, candidate: dict, user_id: str) -> dict:
        """Analyze compatibility between job posting and candidate profile."""
        work_experience, education = self._extract_details(candidate)
        prompt = self._get_prompt(job, candidate, work_experience, education)

        for attempt in range(2):
            try:
                response = await _create_chat_completion(
                    model=settings.OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": "You are an expert AI recruiter specializing in candidate evaluation."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.2,
                    max_tokens=1500,
                    timeout=60,
                )

                # Extract token usage
                self.update_token_usage_per_user(user_id, settings.OPENAI_MODEL, response)

                # Clean and parse response
                content = self._clean_response(response.choices[0].message.content)
                json_match = re.search(r"\{.*\}", content, re.DOTALL)

                if not json_match:
                    raise ValueError("No valid JSON found in LLM response")

                analysis = json.loads(json_match.group(0))
                self._validate_schema(analysis)

                # Clamp scores
                analysis["overall_score"] = max(0, min(100, analysis["overall_score"]))

                return analysis

            except (ValidationError, json.JSONDecodeError, Exception) as e:
                logger.warning("Attempt %d failed: %s", attempt + 1, str(e))
                if attempt == 1:
                    logger.error("Final failure in compatibility analysis.")
                    raise Exception(f"Compatibility analysis failed: {str(e)}")


class AdvancedATSAnalyzer(_AIParser):
    """Evaluates a resume PDF with or without a JD using ATS heuristics and GPT-4 Turbo."""

    def __init__(self):
        """Initialize the CompatibilityAnalyzer."""
        super().__init__()

    def build_prompt(self, resume_text: str, candidate: dict, role_context: str) -> str:
        return f"""
            You are an AI-powered ATS resume evaluator reviewing a resume for a {role_context} position.
            Use the structured metadata, extracted skills, experience, and resume content to provide a deep evaluation.

            --- RESUME METADATA ---
            Name: {candidate.get("name")}
            Email: {candidate.get("email")}
            Phone: {candidate.get("primary_phone")}
            Designation: {candidate.get("designation")}
            Education: {[edu['degree'] + ' from ' + edu['institution'] for edu in candidate.get('education', [])]}
            Total Experience: {candidate.get("total_experience")}
            Technical Skills: {', '.join(candidate.get("technical_skills", []))}
            Soft Skills: {', '.join(candidate.get("soft_skills", []))}
            Certifications: {[cert['name'] for cert in candidate.get('certifications', [])]}

            --- ATS HEURISTICS ---
            - File Type: {candidate.get('metadata', {}).get('file_type')}
            - File Size: {candidate.get('metadata', {}).get('file_size_kb')} KB
            - Page Count: {candidate.get('metadata', {}).get('pages')}
            - Word Count: {candidate.get('metadata', {}).get('word_count')}
            - Personal Pronoun Count: {candidate.get('metadata', {}).get('personal_pronoun_count')}
            - Reading Level: {candidate.get('metadata', {}).get('reading_level')}
            - Vocabulary Level: {candidate.get('metadata', {}).get('vocabulary_level')}
            - Frequent Words: {', '.join(candidate.get('metadata', {}).get('frequent_words', []))}

            Evaluate this resume based on formatting, clarity, completeness, and skill coverage.

            Provide your output in these 4 sections:

            1. **ATS Score (0–100)**:
            - Based on formatting, contact info, skills, education, experience, pronouns, readability

            2. **Skills Summary**:
            - Extract soft and hard skills
            - Skills Efficiency Score = (Hard + Soft skills) / Word Count * 100
            - Identify measurable achievements

            3. **Red Flags**:
            - Missing sections, vague job titles, pronoun usage, poor formatting, etc.

            4. **Recommendations**:
            - Actionable tips to improve the resume for ATS optimization

            --- FULL RESUME TEXT ---
            {resume_text}

            --- RESPONSE FORMAT ---
            Respond in valid JSON with the following structure:
            {{
            "ats_score": <float>,
            "skills_summary": {{
                "measurable_achievements": [...],
                "hard_skills": [...],
                "soft_skills": [...],
                "skills_efficiency_score": <float>
            }},
            "red_flags": [...],
            "recommendations": [...]
            }}
        """

    def _validate_schema(self, analysis: dict) -> None:
        """Validate the analysis response matches expected schema

        Args:
            analysis: Dictionary containing analysis results

        Raises:
            ValidationError: If schema validation fails
        """
        schema = {
            "type": "object",
            "required": ["ats_score", "skills_summary", "red_flags", "recommendations"],
            "properties": {
                "ats_score": {"type": "number"},
                "skills_summary": {
                    "type": "object",
                    "required": ["measurable_achievements", "hard_skills", "soft_skills", "skills_efficiency_score"],
                    "properties": {
                        "measurable_achievements": {"type": "array", "items": {"type": "string"}},
                        "hard_skills": {"type": "array", "items": {"type": "string"}},
                        "soft_skills": {"type": "array", "items": {"type": "string"}},
                        "skills_efficiency_score": {"type": "number"},
                    },
                },
                "red_flags": {"type": "array", "items": {"type": "string"}},
                "recommendations": {"type": "array", "items": {"type": "string"}},
            },
        }
        validate(instance=analysis, schema=schema)

    async def analyze(self, role: str, pdf_text: str, candidate_data: dict, user_id: str) -> candidate.ATSEvaluation:
        """Analyze resume text and generate ATS evaluation

        Args:
            role: Job role being applied for
            pdf_text: Raw text extracted from resume PDF
            metadata: Dictionary containing resume metadata/heuristics
            user_id: ID of user making request

        Returns:
            ATSEvaluation object containing analysis results

        Raises:
            ValueError: If OpenAI API call fails or response cannot be parsed
            ValidationError: If response schema validation fails
        """
        try:
            # Build and send prompt to OpenAI
            prompt = self.build_prompt(pdf_text, candidate_data, role)

            try:
                response = await _create_chat_completion(
                    model=settings.OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": "You are a strict JSON responder and expert ATS resume evaluator."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.3,
                    max_tokens=1500,
                    timeout=60,
                )
            except Exception as e:
                logger.error(f"OpenAI API call failed: {str(e)}")
                raise ValueError(f"Failed to get analysis from OpenAI: {str(e)}")

            # Track token usage for billing
            self.update_token_usage_per_user(user_id, settings.OPENAI_MODEL, response)

            # Clean and parse response
            content = self._clean_response(response.choices[0].message.content)
            json_match = re.search(r"\{.*\}", content, re.DOTALL)
            if not json_match:
                raise ValueError("Could not extract JSON from OpenAI response")

            try:
                analysis = json.loads(json_match.group(0))
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in OpenAI response: {str(e)}")
                raise ValueError(f"Failed to parse OpenAI response as JSON: {str(e)}")

            try:
                self._validate_schema(analysis)
            except ValidationError as e:
                logger.error(f"Schema validation failed: {str(e)}")
                raise ValueError(f"OpenAI response failed schema validation: {str(e)}")

            return analysis

        except Exception as e:
            logger.error(f"Resume analysis failed: {str(e)}")
            raise ValueError(f"Resume analysis failed: {str(e)}")


class _ImageTranslator(_AIParser):
    """Translate image content into text using llm"""

    def __init__(self):
        """Initialize the ImageTranslator."""
        super().__init__()

    async def _parse_img_text(self, base64_encoded_imgs: str, user_id: str):
        # tapering the images to prevent API overflows, usually A cv will have 2-3 pages max.
        base64_encoded_imgs = base64_encoded_imgs[:10]
        payload_content = [
            {
                "type": "text",
                "text": f"{_CVParserClient(is_image=True).prompt_template}",
            }
        ]

        content_dict_img = [
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
            }
            for base64_image in base64_encoded_imgs
        ]
        payload_content.extend(content_dict_img)
        payload_messages = [{"role": "user", "content": payload_content}]
        try:
            response = await _create_chat_completion(
                model=settings.OPENAI_MODEL,
                messages=payload_messages,
                temperature=0.2,
                max_tokens=4000,
                timeout=80,
            )

            # Extract token usage
            self.update_token_usage_per_user(user_id, settings.OPENAI_MODEL, response)
            # Extract JSON object
            parsed_data = response.choices[0].message.content.strip()
            parsed_json = Toolkit.extract_json(parsed_data)
            # Ensure certifications are strings
            json_str = json.dumps(parsed_json)
            return candidate.CVParseResponse.model_validate_json(json_str)

        except Exception as e:
            logger.error(f"Failed to AI parse the images, \n {e}")
            raise


# class _AudioAnalyzer(_AIParser):
#     """Analyze audio content using llm"""

#     def __init__(self):
#         """Initialize the AudioAnalyzer."""
#         super().__init__()

#     async def parse_call_recording(self, file_path: str) -> Optional[str]:
#         """Parse the call recording with AI and update transcription in the database"""
#         try:
#             audio = open(file=file_path, mode="rb")

#             # TODO: Get language from somewhere else
#             # Using optimized settings for best transcription quality:
#             # - temperature=0.0 for more focused/deterministic output
#             # - prompt parameter to provide context and improve accuracy
#             # - language parameter to help with speech recognition
#             transcript = await _create_transcription(
#                 file=audio,
#                 response_format="text",
#                 temperature=0.0,
#                 prompt="This is a professional conversation between a recruiter and a job candidate.",
#                 language="en",
#                 timeout=120,  # Increased timeout for longer recordings
#             )

#             return transcript
#         finally:
#             audio.close()


class AIServices:
    """Singleton class to manage all AI service instances"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize_services()
        return cls._instance

    def _initialize_services(self):
        """Initialize all AI service instances"""
        self.cv_parser = _CVParserClient()
        self.jd_parser = _JobDescriptionParserClient()
        self.compatibility_analyzer = _CompatibilityAnalyzer()
        self.image_translator = _ImageTranslator()
        self.candidate_questions = _CandidateSpecificQuestionsClient()
        self.ats_analyzer = AdvancedATSAnalyzer()
        # self.audio_analyzer = _AudioAnalyzer()


# Create singleton instance
ai_services = AIServices()
