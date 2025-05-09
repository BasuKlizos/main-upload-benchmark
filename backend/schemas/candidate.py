from typing import List, Optional

from pydantic import BaseModel, Field


class Education(BaseModel):
    degree: str
    institution: str
    graduation_year: int
    score: Optional[str]


class WorkExperience(BaseModel):
    company_name: str
    position: str
    duration: str
    is_remote: Optional[bool]
    responsibilities: List[str]
    responsibility_summary: str


class SocialProfiles(BaseModel):
    github: str
    linkedin: str
    twitter: str
    leetcode: str
    others: List[str]


class Language(BaseModel):
    name: str
    proficiency: str


class Certification(BaseModel):
    name: str
    issuer: Optional[str]
    issue_date: Optional[str]
    expiry_date: Optional[str]
    verification_link: Optional[str]


class PersonalDetails(BaseModel):
    date_of_birth: Optional[str]
    age: Optional[int]
    gender: Optional[str]


class Project(BaseModel):
    name: str
    project_type: str
    description: Optional[str] = None
    technologies: Optional[List[str]] = None


class CVParseResponse(BaseModel):
    name: str
    email: str
    secondary_email: Optional[str]
    primary_phone: Optional[str]
    secondary_phone: Optional[str]
    cv_directory_link: Optional[str] = None
    unique_id: Optional[str] = None
    designation: Optional[str] = None
    location: Optional[str]
    personal_details: Optional[PersonalDetails]
    social_profiles: SocialProfiles
    education: List[Education]
    work_experience: List[WorkExperience]
    total_experience: Optional[str]
    technical_skills: List[str]
    soft_skills: List[str]
    languages: List[Language]
    certifications: List[Certification]
    projects: Optional[List[Project]]
    metadata: Optional[dict] = None


class ATSEvaluationSkillsSummary(BaseModel):
    measurable_achievements: List[str]
    hard_skills: List[str]
    soft_skills: List[str]
    skills_efficiency_score: float


class ATSEvaluation(BaseModel):
    ats_score: float
    skills_summary: ATSEvaluationSkillsSummary
    red_flags: List[str]
    recommendations: List[str]


class AddGeneralCandidate(BaseModel):
    name: str = Field(..., description="Name of the candidate")
    email: str = Field(..., description="Email of the candidate")
    job_id: str = Field(..., description="ID of the job")
    company_id: str = Field(..., description="ID of the company")
    phone_no: Optional[str] = Field(None, description="Phone number of the candidate")
    status: Optional[str] = Field("new", description="Status of the candidate")
    type: Optional[str] = Field("general", description="Type of candidate")
